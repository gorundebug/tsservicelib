import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import {
  Client,
  ScheduleAlreadyRunning,
  ScheduleOverlapPolicy,
  type WorkflowHandle
} from "@temporalio/client";
import { cancellationSignal, heartbeat } from "@temporalio/activity";
import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from "@temporalio/common";
import { NativeConnection, Runtime, Worker } from "@temporalio/worker";

import {
  DataConnectorType,
  type DataConnectorConfig,
  type EndpointConfig,
  type TemporalDataConnectorConfig,
  type TemporalEndpointConfig
} from "../../runtime/config/index.js";
import type { Context, MessageContext } from "../../runtime/context.js";
import type { ManagedDataConnector } from "../../runtime/data-connector.js";
import {
  DurableCallContext,
  DurableCallEvent,
  runDurableCallActivity,
  type DurableCallDiagnostics
} from "../../runtime/durable-call-context.js";
import type { RuntimeEnvironment } from "../../runtime/environment/index.js";
import { err, str, type Int64CounterVec } from "../../runtime/environment/index.js";
import { normalizeTemporalPriority } from "../../runtime/schedule.js";
import {
  ENDPOINT_WORKFLOW_TYPE,
  type EndpointEnvelope,
  type EndpointResult,
  type EndpointWireEnvelope,
  type EndpointWireResult,
  type EndpointWorkflowRequest
} from "./contracts.js";
import {
  currentTemporalActivityMessageContext,
  runWithTemporalSubmissionContext,
  temporalActivityInterceptors,
  temporalWorkflowClientInterceptor
} from "./context-propagation.js";

const MANAGED_BY = "servicelib.managedBy";
const OWNER = "servicelib.owner";
const MESSAGE_ID = "servicelib.messageId";
const SDK_METRICS_BIND_ADDRESS_ENVIRONMENT = "TEMPORAL_SDK_METRICS_BIND_ADDRESS";
let sdkMetricsBindAddress: string | undefined;

export function temporalCronExpression(expression: string): string {
  return `0 ${expression.trim().split(/\s+/u).join(" ")}`;
}

export type TemporalEndpointHandler = (
  envelope: EndpointEnvelope,
  context: MessageContext,
  cancellationSignal?: AbortSignal
) => Promise<EndpointResult>;

interface EndpointRegistration {
  readonly endpointId: number;
  readonly activityType: string;
  readonly handler?: TemporalEndpointHandler;
}

export class TemporalConnector implements ManagedDataConnector {
  readonly #environment: RuntimeEnvironment;
  readonly #endpoints = new Map<number, EndpointRegistration>();
  readonly #activityEvents: Int64CounterVec;
  #connection: NativeConnection | undefined;
  #client: Client | undefined;
  #workers: Worker[] = [];
  #workerRuns: Promise<void>[] = [];
  #started = false;
  public readonly id: number;
  public readonly name: string;

  public constructor(connectorId: number, environment: RuntimeEnvironment) {
    const config = environment.runtimeConfig().dataConnectorById(connectorId);
    if (config?.type !== DataConnectorType.Temporal) {
      throw new Error(`data connector ${String(connectorId)} is not Temporal`);
    }
    this.id = connectorId;
    this.name = config.name;
    this.#environment = environment;
    this.#activityEvents = environment
      .metrics()
      .scope("temporal_activity", { connector: this.name })
      .counterVec("events_total", "Total number of Temporal Activity lifecycle events");
  }

  public registerEndpoint(endpointId: number, handler: TemporalEndpointHandler): void {
    if (this.#started) throw new Error("cannot register endpoint after Temporal start");
    const registration = this.#endpoints.get(endpointId) ?? this.endpointRegistration(endpointId);
    if (registration.handler !== undefined) {
      throw new Error(`Temporal endpoint ${String(endpointId)} is already registered`);
    }
    this.#endpoints.set(endpointId, { ...registration, handler });
  }

  public registerEndpointSubmission(endpointId: number): void {
    if (!this.#endpoints.has(endpointId)) {
      this.#endpoints.set(endpointId, this.endpointRegistration(endpointId));
    }
  }

  public async start(context: Context): Promise<void> {
    if (this.#started) return;
    context.signal().throwIfAborted();
    const config = this.config();
    installSdkMetricsRuntime();
    const tls = await tlsOptions(config);
    const connection = await abortable(
      NativeConnection.connect({
        address: config.address,
        ...(tls === undefined ? {} : { tls }),
        ...(config.apiKey === "" ? {} : { apiKey: config.apiKey }),
        ...(context.remainingMs() === undefined ? {} : { connectTimeout: context.remainingMs() })
      }),
      context.signal()
    );
    this.#connection = connection;
    this.#client = new Client({
      connection,
      namespace: config.namespace,
      interceptors: { workflow: [temporalWorkflowClientInterceptor] },
      ...(config.identity === "" ? {} : { identity: config.identity })
    });
    try {
      for (const [taskQueue, activities] of this.queueActivities()) {
        const worker = await Worker.create({
          connection,
          namespace: config.namespace,
          taskQueue,
          activities,
          workflowsPath: fileURLToPath(new URL("./workflows.js", import.meta.url)),
          interceptors: {
            activity: [temporalActivityInterceptors],
            workflowModules: [
              fileURLToPath(new URL("./workflow-context-interceptor.js", import.meta.url))
            ]
          },
          ...(config.identity === "" ? {} : { identity: config.identity }),
          ...(config.maxConcurrentActivities > 0
            ? { maxConcurrentActivityTaskExecutions: config.maxConcurrentActivities }
            : {}),
          ...(config.maxConcurrentWorkflows > 0
            ? { maxConcurrentWorkflowTaskExecutions: config.maxConcurrentWorkflows }
            : {})
        });
        this.#workers.push(worker);
        this.#workerRuns.push(worker.run());
      }
      await ensureWorkersRunning(this.#workerRuns);
      this.#started = true;
      for (const endpointId of this.#endpoints.keys()) {
        const endpoint = this.endpointConfig(endpointId);
        if (endpoint.enabled && endpoint.schedule !== "") await this.ensureSchedule(endpoint);
      }
    } catch (error: unknown) {
      await this.shutdownWorkers();
      await connection.close();
      this.#connection = undefined;
      this.#client = undefined;
      throw error;
    }
  }

  public async stopAdmission(context: Context): Promise<void> {
    void context;
    await this.shutdownWorkers();
  }

  public async stop(context: Context): Promise<void> {
    void context;
    await this.shutdownWorkers();
    this.#started = false;
    const connection = this.#connection;
    this.#client = undefined;
    this.#connection = undefined;
    if (connection !== undefined) await connection.close();
  }

  public async submitEndpoint(
    context: MessageContext,
    endpointId: number,
    envelope: EndpointEnvelope,
    waitForResult: boolean
  ): Promise<EndpointResult> {
    if (!this.#started) throw new Error(`Temporal connector ${this.name} is not started`);
    const registration = this.#endpoints.get(endpointId);
    if (registration === undefined) {
      throw new Error(`Temporal endpoint ${String(endpointId)} is not registered`);
    }
    const config = this.endpointConfig(endpointId);
    if (!config.enabled) throw new Error(`Temporal endpoint ${config.name} is disabled`);
    if (envelope.messageId === "") throw new Error("Temporal endpoint message ID is empty");
    const request = endpointRequest(registration, config, envelope);
    const owner = endpointOwner(this.name, config.name);
    const handle = await runWithTemporalSubmissionContext(context, () =>
      this.client().workflow.start(ENDPOINT_WORKFLOW_TYPE, {
        args: [request],
        workflowId: endpointWorkflowId(this.name, config.name, envelope.messageId),
        taskQueue: config.taskQueue,
        ...(config.workflowExecutionTimeout > 0
          ? { workflowExecutionTimeout: config.workflowExecutionTimeout }
          : {}),
        workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
        workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
        memo: ownershipMemo(owner, envelope.messageId),
        priority: { priorityKey: request.priority }
      })
    );
    await validateWorkflowOwnership(handle, ENDPOINT_WORKFLOW_TYPE, owner, envelope.messageId);
    if (!waitForResult) return { payload: new Uint8Array() };
    const result = (await handle.result()) as EndpointWireResult;
    return { payload: bytesFromWire(result.payload) };
  }

  private queueActivities(): Map<string, Record<string, (value: unknown) => Promise<unknown>>> {
    const queues = new Map<string, Record<string, (value: unknown) => Promise<unknown>>>();
    for (const registration of this.#endpoints.values()) {
      const config = this.endpointConfig(registration.endpointId);
      if (!config.enabled || registration.handler === undefined) continue;
      const handler = registration.handler;
      const activities = queues.get(config.taskQueue) ?? {};
      queues.set(config.taskQueue, activities);
      activities[registration.activityType] = async (value) => {
        const signal = cancellationSignal();
        const envelope = endpointEnvelopeFromWire(value as EndpointWireEnvelope);
        const durable = new DurableCallContext(
          envelope.messageId,
          heartbeat,
          this.activityDiagnostics(
            "endpoint",
            String(registration.endpointId),
            currentTemporalActivityMessageContext()
          )
        );
        const context = currentTemporalActivityMessageContext()
          .withExternalCancellation(signal)
          .withDurableCallContext(durable);
        const result = await runDurableCallActivity(durable, () =>
          handler(envelope, context, signal)
        );
        return { payload: bytesToWire(result.payload) } satisfies EndpointWireResult;
      };
    }
    return queues;
  }

  private activityDiagnostics(
    boundary: string,
    target: string,
    context: Context
  ): DurableCallDiagnostics {
    return (event, failure): void => {
      this.#activityEvents.with({ boundary, target, event }).inc(context);
      if (failure === undefined) return;
      const fields = [
        str("connector", this.name),
        str("boundary", boundary),
        str("target", target),
        str("event", event),
        err(failure)
      ] as const;
      if (event === DurableCallEvent.LateHeartbeat) {
        this.#environment.log().warn(context, "Temporal Activity lifecycle misuse", ...fields);
      } else {
        this.#environment.log().error(context, "Temporal Activity failed", ...fields);
      }
    };
  }

  private async ensureSchedule(config: TemporalEndpointConfig): Promise<void> {
    const registration = this.#endpoints.get(config.id);
    if (registration?.handler === undefined) return;
    const owner = endpointOwner(this.name, config.name);
    const request = endpointRequest(registration, config, {
      version: 1,
      endpointId: config.id,
      messageId: "",
      streamId: "",
      priority: 0,
      deadlineUnixMillis: 0,
      scheduled: true,
      scheduleId: config.scheduleId,
      scheduledAtUnixMillis: 0,
      firedAtUnixMillis: 0,
      payload: new Uint8Array()
    });
    try {
      await this.client().schedule.create({
        scheduleId: config.scheduleId,
        spec: {
          cronExpressions: [temporalCronExpression(config.schedule)],
          timezone: config.timezone
        },
        action: {
          type: "startWorkflow",
          workflowType: ENDPOINT_WORKFLOW_TYPE,
          workflowId: `${identityName(this.name)}/schedule/${identityName(config.name)}`,
          taskQueue: config.taskQueue,
          args: [request],
          memo: ownershipMemo(owner, config.scheduleId),
          ...(config.workflowExecutionTimeout > 0
            ? { workflowExecutionTimeout: config.workflowExecutionTimeout }
            : {})
        },
        policies: {
          overlap:
            config.overlapPolicy === "Allow"
              ? ScheduleOverlapPolicy.ALLOW_ALL
              : ScheduleOverlapPolicy.SKIP,
          catchupWindow: config.missedRunPolicy === "FireOnce" ? 365 * 24 * 60 * 60 * 1000 : 10_000
        },
        memo: ownershipMemo(owner, config.scheduleId)
      });
    } catch (error: unknown) {
      if (!(error instanceof ScheduleAlreadyRunning)) throw error;
      const description = await this.client().schedule.getHandle(config.scheduleId).describe();
      validateMemo(description.memo, owner, config.scheduleId);
      if (
        description.action.workflowType !== ENDPOINT_WORKFLOW_TYPE ||
        description.action.taskQueue !== config.taskQueue
      ) {
        throw new Error(`Temporal schedule ${config.scheduleId} ownership collision`, {
          cause: error
        });
      }
    }
  }

  private endpointRegistration(endpointId: number): EndpointRegistration {
    const config = this.endpointConfig(endpointId);
    if (config.idDataConnector !== this.id) {
      throw new Error(`endpoint ${String(endpointId)} does not belong to connector ${this.name}`);
    }
    return {
      endpointId,
      activityType: `${identityName(this.name)}.endpoint.${identityName(config.name)}.v1`
    };
  }

  private config(): TemporalDataConnectorConfig {
    const config = this.#environment.runtimeConfig().dataConnectorById(this.id);
    if (!isTemporalConnectorConfig(config)) {
      throw new Error(`Temporal connector ${this.name} configuration not found`);
    }
    return config;
  }

  private endpointConfig(endpointId: number): TemporalEndpointConfig {
    const config = this.#environment.runtimeConfig().endpointById(endpointId);
    if (!isTemporalEndpointConfig(config)) {
      throw new Error(`Temporal endpoint configuration ${String(endpointId)} not found`);
    }
    return config;
  }

  private client(): Client {
    if (this.#client === undefined)
      throw new Error(`Temporal connector ${this.name} is not started`);
    return this.#client;
  }

  private async shutdownWorkers(): Promise<void> {
    for (const worker of this.#workers) worker.shutdown();
    await Promise.allSettled(this.#workerRuns);
    this.#workers = [];
    this.#workerRuns = [];
  }
}

export function makeTemporalConnector(
  connectorId: number,
  environment: RuntimeEnvironment
): TemporalConnector {
  const existing = environment.managedDataConnectorById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof TemporalConnector)) {
      throw new Error(`managed connector ${String(connectorId)} is not Temporal`);
    }
    return existing;
  }
  const connector = new TemporalConnector(connectorId, environment);
  environment.addManagedDataConnector(connector);
  return connector;
}

export function endpointWorkflowId(
  connectorName: string,
  endpointName: string,
  messageId: string
): string {
  return `${identityName(connectorName)}/endpoint/${identityName(endpointName)}/${opaqueIdentityComponent(messageId)}`;
}

function endpointOwner(connectorName: string, endpointName: string): string {
  return `${identityName(connectorName)}/endpoint/${identityName(endpointName)}/v1`;
}

function endpointRequest(
  registration: EndpointRegistration,
  config: TemporalEndpointConfig,
  envelope: EndpointEnvelope
): EndpointWorkflowRequest {
  return {
    activityType: registration.activityType,
    activityStartToCloseTimeout: config.activityStartToCloseTimeout,
    activityHeartbeatTimeout: config.activityHeartbeatTimeout,
    maximumAttempts: config.maximumAttempts,
    priority: normalizeTemporalPriority(envelope.priority),
    envelope: endpointEnvelopeToWire(envelope)
  };
}

function endpointEnvelopeToWire(envelope: EndpointEnvelope): EndpointWireEnvelope {
  return { ...envelope, payload: bytesToWire(envelope.payload) };
}

function endpointEnvelopeFromWire(envelope: EndpointWireEnvelope): EndpointEnvelope {
  return { ...envelope, payload: bytesFromWire(envelope.payload) };
}

function bytesToWire(value: Uint8Array): readonly number[] {
  return Array.from(value);
}

function bytesFromWire(value: readonly number[]): Uint8Array {
  if (
    !Array.isArray(value) ||
    value.some((item) => !Number.isInteger(item) || item < 0 || item > 255)
  ) {
    throw new TypeError("invalid Temporal byte payload");
  }
  return Uint8Array.from(value);
}

function ownershipMemo(owner: string, messageId: string): Record<string, unknown> {
  return { [MANAGED_BY]: "servicelib", [OWNER]: owner, [MESSAGE_ID]: messageId };
}

function opaqueIdentityComponent(value: string): string {
  return encodeURIComponent(value).replaceAll(
    /[!'()*]/g,
    (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

function identityName(value: string): string {
  const words: string[] = [];
  let current: string[] = [];
  const characters = Array.from(value);
  for (const [index, character] of characters.entries()) {
    if (/\s/u.test(character) || "_-/.".includes(character)) {
      if (current.length > 0) {
        words.push(current.join(""));
        current = [];
      }
      continue;
    }
    if (!/[\p{L}\p{N}]/u.test(character)) continue;
    const upper = character.toUpperCase() === character && character.toLowerCase() !== character;
    if (current.length > 0 && upper) {
      const previous = current.at(-1) ?? "";
      const previousUpper =
        previous.toUpperCase() === previous && previous.toLowerCase() !== previous;
      const next = characters[index + 1];
      const nextLower = next?.toLowerCase() === next && next?.toUpperCase() !== next;
      if (!previousUpper || nextLower) {
        words.push(current.join(""));
        current = [];
      }
    }
    current.push(character);
  }
  if (current.length > 0) words.push(current.join(""));
  return words.map((word) => word.toLowerCase()).join("_");
}

async function validateWorkflowOwnership(
  handle: WorkflowHandle,
  workflowType: string,
  owner: string,
  messageId: string
): Promise<void> {
  const description = await handle.describe();
  if (description.type !== workflowType) throw new Error("Temporal workflow ownership collision");
  validateMemo(description.memo, owner, messageId);
}

function validateMemo(
  memo: Record<string, unknown> | undefined,
  owner: string,
  messageId: string
): void {
  if (
    memo?.[MANAGED_BY] !== "servicelib" ||
    memo[OWNER] !== owner ||
    memo[MESSAGE_ID] !== messageId
  ) {
    throw new Error(`Temporal ownership collision for ${owner}`);
  }
}

function isTemporalConnectorConfig(
  config: DataConnectorConfig | undefined
): config is TemporalDataConnectorConfig {
  return (
    config?.type === DataConnectorType.Temporal && "address" in config && "namespace" in config
  );
}

function isTemporalEndpointConfig(
  config: EndpointConfig | undefined
): config is TemporalEndpointConfig {
  return config !== undefined && "taskQueue" in config && typeof config.taskQueue === "string";
}

function installSdkMetricsRuntime(): void {
  const address = process.env[SDK_METRICS_BIND_ADDRESS_ENVIRONMENT]?.trim();
  if (address === undefined || address === "") return;
  if (sdkMetricsBindAddress !== undefined) {
    if (sdkMetricsBindAddress !== address) {
      throw new Error(
        `Temporal SDK metrics already listen on ${sdkMetricsBindAddress}, cannot also use ${address}`
      );
    }
    return;
  }
  Runtime.install({
    telemetryOptions: {
      metrics: { prometheus: { bindAddress: address, useSecondsForDurations: true } }
    }
  });
  sdkMetricsBindAddress = address;
}

async function tlsOptions(config: TemporalDataConnectorConfig): Promise<
  | true
  | {
      readonly serverNameOverride?: string;
      readonly serverRootCACertificate?: Uint8Array;
      readonly clientCertPair?: { readonly crt: Uint8Array; readonly key: Uint8Array };
    }
  | undefined
> {
  if (!config.tlsEnabled) return undefined;
  if ((config.tlsCertFile === "") !== (config.tlsKeyFile === "")) {
    throw new Error(`Temporal connector ${config.name} requires both TLS cert and key`);
  }
  const ca = config.tlsCaFile === "" ? undefined : await readFile(config.tlsCaFile);
  const pair =
    config.tlsCertFile === ""
      ? undefined
      : {
          crt: await readFile(config.tlsCertFile),
          key: await readFile(config.tlsKeyFile)
        };
  if (config.tlsServerName === "" && ca === undefined && pair === undefined) return true;
  return {
    ...(config.tlsServerName === "" ? {} : { serverNameOverride: config.tlsServerName }),
    ...(ca === undefined ? {} : { serverRootCACertificate: ca }),
    ...(pair === undefined ? {} : { clientCertPair: pair })
  };
}

async function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) throw abortReason(signal);
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => {
      reject(abortReason(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    void promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    );
  });
}

async function ensureWorkersRunning(workerRuns: readonly Promise<void>[]): Promise<void> {
  if (workerRuns.length === 0) return;
  const state = await Promise.race([
    Promise.all(workerRuns).then(() => "stopped" as const),
    new Promise<"running">((resolve) => {
      setImmediate(resolve, "running");
    })
  ]);
  if (state === "stopped") throw new Error("Temporal workers stopped during startup");
}

function abortReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error("Temporal operation aborted");
}
