import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import {
  Client,
  ScheduleAlreadyRunning,
  ScheduleOverlapPolicy,
  type WorkflowHandle
} from "@temporalio/client";
import { cancellationSignal } from "@temporalio/activity";
import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from "@temporalio/common";
import { NativeConnection, Worker } from "@temporalio/worker";

import {
  DataConnectorType,
  type DataConnectorConfig,
  type EndpointConfig,
  type InputStreamConfig,
  type StreamConfig,
  type DurableCallSemanticsConfig,
  type TemporalDataConnectorConfig,
  type TemporalEndpointConfig
} from "../config/index.js";
import type { Context } from "../context.js";
import type {
  DurableEnvelope,
  DurableLinkHandler,
  DurableLinkId,
  DurableTransport
} from "../durable.js";
import type { RuntimeEnvironment } from "../environment/index.js";
import { normalizeTemporalPriority } from "../schedule.js";
import {
  DURABLE_WORKFLOW_TYPE,
  ENDPOINT_WORKFLOW_TYPE,
  type DurableWorkflowRequest,
  type EndpointEnvelope,
  type EndpointResult,
  type EndpointWireEnvelope,
  type EndpointWireResult,
  type EndpointWorkflowRequest
} from "./contracts.js";

const MANAGED_BY = "servicegen.managedBy";
const OWNER = "servicegen.owner";
const CALL_ID = "servicegen.callId";

export type TemporalEndpointHandler = (
  envelope: EndpointEnvelope,
  cancellationSignal?: AbortSignal
) => Promise<EndpointResult>;

interface LinkRegistration {
  readonly link: DurableLinkId;
  readonly activityType: string;
  readonly handler: DurableLinkHandler;
}

interface EndpointRegistration {
  readonly endpointId: number;
  readonly activityType: string;
  readonly handler?: TemporalEndpointHandler;
}

export class TemporalConnector implements DurableTransport {
  readonly #environment: RuntimeEnvironment;
  readonly #links = new Map<string, LinkRegistration>();
  readonly #endpoints = new Map<number, EndpointRegistration>();
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
  }

  public registerLink(link: DurableLinkId, handler: DurableLinkHandler): void {
    if (this.#started) throw new Error("cannot register DurableCall after Temporal start");
    const key = linkKey(link);
    if (this.#links.has(key)) throw new Error(`durable link ${key} is already registered`);
    const config = this.linkConfig(link);
    if (config.idDataConnector !== this.id) {
      throw new Error(`durable link ${key} does not belong to connector ${this.name}`);
    }
    const serviceId = this.#environment.serviceConfig().id;
    this.#links.set(key, {
      link,
      activityType: `servicegen.durable.${String(serviceId)}.${String(link.from)}.${String(link.to)}.v1`,
      handler
    });
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
    const connection = await this.connect(config, context);
    this.#connection = connection;
    this.#client = new Client({
      connection,
      namespace: config.namespace,
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
      for (const endpointId of this.#endpoints.keys()) {
        const endpoint = this.endpointConfig(endpointId);
        if (endpoint.enabled && endpoint.schedule !== "") await this.ensureSchedule(endpoint);
      }
      this.#started = true;
    } catch (error: unknown) {
      await this.shutdownWorkers();
      await connection.close();
      this.#connection = undefined;
      this.#client = undefined;
      throw error;
    }
  }

  public async stopAdmission(_context: Context): Promise<void> {
    void _context;
    await this.shutdownWorkers();
  }

  public async stop(_context: Context): Promise<void> {
    void _context;
    await this.shutdownWorkers();
    this.#started = false;
    const connection = this.#connection;
    this.#client = undefined;
    this.#connection = undefined;
    if (connection !== undefined) await connection.close();
  }

  public async submitLink(link: DurableLinkId, envelope: DurableEnvelope): Promise<void> {
    if (!this.#started) throw new Error(`Temporal connector ${this.name} is not started`);
    const registration = this.#links.get(linkKey(link));
    if (registration === undefined)
      throw new Error(`durable link ${linkKey(link)} is not registered`);
    const policy = this.linkConfig(link);
    const request: DurableWorkflowRequest = {
      activityType: registration.activityType,
      activityStartToCloseTimeout: policy.activityStartToCloseTimeout,
      activityHeartbeatTimeout: policy.activityHeartbeatTimeout,
      maximumAttempts: policy.maximumAttempts,
      priority: normalizeTemporalPriority(envelope.priority),
      envelope: durableEnvelopeToWire(envelope)
    };
    const serviceId = this.#environment.serviceConfig().id;
    const owner = `servicegen/${String(serviceId)}/link/${String(link.from)}/${String(link.to)}/v1`;
    const handle = await this.client().workflow.start(DURABLE_WORKFLOW_TYPE, {
      args: [request],
      workflowId: `servicegen/durable/${String(serviceId)}/${String(link.from)}/${String(link.to)}/${envelope.callId}`,
      taskQueue: policy.taskQueue,
      ...(policy.workflowExecutionTimeout > 0
        ? { workflowExecutionTimeout: policy.workflowExecutionTimeout }
        : {}),
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
      memo: ownershipMemo(owner, envelope.callId),
      priority: { priorityKey: request.priority }
    });
    await validateWorkflowOwnership(handle, DURABLE_WORKFLOW_TYPE, owner, envelope.callId);
  }

  public async submitEndpoint(
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
    const request = endpointRequest(registration, config, envelope);
    const serviceId = this.#environment.serviceConfig().id;
    const owner = `servicegen/${String(serviceId)}/endpoint/${String(endpointId)}/v1`;
    const handle = await this.client().workflow.start(ENDPOINT_WORKFLOW_TYPE, {
      args: [request],
      workflowId: `servicegen/endpoint/${String(serviceId)}/${String(endpointId)}/${envelope.executionId}`,
      taskQueue: config.taskQueue,
      ...(config.workflowExecutionTimeout > 0
        ? { workflowExecutionTimeout: config.workflowExecutionTimeout }
        : {}),
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
      memo: ownershipMemo(owner, envelope.executionId),
      priority: { priorityKey: request.priority }
    });
    await validateWorkflowOwnership(handle, ENDPOINT_WORKFLOW_TYPE, owner, envelope.executionId);
    if (!waitForResult) return { payload: new Uint8Array() };
    const result = (await handle.result()) as EndpointWireResult;
    return { payload: bytesFromWire(result.payload) };
  }

  private async connect(
    config: TemporalDataConnectorConfig,
    context: Context
  ): Promise<NativeConnection> {
    const tls = await tlsOptions(config);
    const connect = NativeConnection.connect({
      address: config.address,
      ...(tls === undefined ? {} : { tls }),
      ...(config.apiKey === "" ? {} : { apiKey: config.apiKey }),
      ...(context.remainingMs() === undefined ? {} : { connectTimeout: context.remainingMs() })
    });
    return abortable(connect, context.signal());
  }

  private queueActivities(): Map<string, Record<string, (value: unknown) => Promise<unknown>>> {
    const queues = new Map<string, Record<string, (value: unknown) => Promise<unknown>>>();
    const queue = (name: string): Record<string, (value: unknown) => Promise<unknown>> => {
      const existing = queues.get(name);
      if (existing !== undefined) return existing;
      const created: Record<string, (value: unknown) => Promise<unknown>> = {};
      queues.set(name, created);
      return created;
    };
    for (const registration of this.#links.values()) {
      queue(this.linkConfig(registration.link).taskQueue)[registration.activityType] = async (
        value
      ) =>
        registration.handler(
          durableEnvelopeFromWire(value as DurableWorkflowRequest["envelope"]),
          cancellationSignal()
        );
    }
    for (const registration of this.#endpoints.values()) {
      const config = this.endpointConfig(registration.endpointId);
      if (!config.enabled || registration.handler === undefined) continue;
      const handler = registration.handler;
      queue(config.taskQueue)[registration.activityType] = async (value) => {
        const result = await handler(
          endpointEnvelopeFromWire(value as EndpointWireEnvelope),
          cancellationSignal()
        );
        return { payload: bytesToWire(result.payload) } satisfies EndpointWireResult;
      };
    }
    return queues;
  }

  private async shutdownWorkers(): Promise<void> {
    for (const worker of this.#workers) worker.shutdown();
    await Promise.allSettled(this.#workerRuns);
    this.#workers = [];
    this.#workerRuns = [];
  }

  private async ensureSchedule(config: TemporalEndpointConfig): Promise<void> {
    const registration = this.#endpoints.get(config.id);
    if (registration?.handler === undefined) return;
    const serviceId = this.#environment.serviceConfig().id;
    const owner = `servicegen/${String(serviceId)}/endpoint/${String(config.id)}/v1`;
    const request = endpointRequest(registration, config, {
      version: 1,
      endpointId: config.id,
      executionId: "",
      streamId: "",
      priority: 0,
      deadlineUnixMillis: 0,
      samplingEnabled: false,
      scheduled: true,
      scheduleId: config.scheduleId,
      scheduledAtUnixMillis: 0,
      firedAtUnixMillis: 0,
      payload: new Uint8Array()
    });
    try {
      await this.client().schedule.create({
        scheduleId: config.scheduleId,
        spec: { cronExpressions: [config.schedule], timezone: config.timezone },
        action: {
          type: "startWorkflow",
          workflowType: ENDPOINT_WORKFLOW_TYPE,
          workflowId: `servicegen/schedule/${String(serviceId)}/${String(config.id)}`,
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
    const inputs = this.#environment
      .runtimeConfig()
      .config()
      .streams.filter((stream) => isInputStreamConfig(stream) && stream.idEndpoint === endpointId);
    if (inputs.length !== 1) {
      throw new Error(
        `Temporal endpoint ${String(endpointId)} must have exactly one input stream; found ${String(inputs.length)}`
      );
    }
    return {
      endpointId,
      activityType: `servicegen.endpoint.${String(inputs[0]?.idService)}.${String(endpointId)}.v1`
    };
  }

  private config(): TemporalDataConnectorConfig {
    const config = this.#environment.runtimeConfig().dataConnectorById(this.id);
    if (!isTemporalConnectorConfig(config)) {
      throw new Error(`Temporal connector ${this.name} configuration not found`);
    }
    return config;
  }

  private linkConfig(link: DurableLinkId): DurableCallSemanticsConfig {
    const semantics = this.#environment.runtimeConfig().link(link.from, link.to)?.callSemantics;
    if (semantics === undefined || !("durableCall" in semantics)) {
      throw new Error(`DurableCall configuration ${linkKey(link)} not found`);
    }
    return semantics.durableCall;
  }

  private endpointConfig(endpointId: number): TemporalEndpointConfig {
    const config = this.#environment.runtimeConfig().endpointById(endpointId);
    if (!isTemporalEndpointConfig(config)) {
      throw new Error(`Temporal endpoint configuration ${String(endpointId)} not found`);
    }
    return config;
  }

  private client(): Client {
    if (this.#client === undefined) {
      throw new Error(`Temporal connector ${this.name} is not started`);
    }
    return this.#client;
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

function isInputStreamConfig(config: StreamConfig): config is InputStreamConfig {
  return config.type === "Input" && "idEndpoint" in config;
}

export function makeTemporalConnector(
  connectorId: number,
  environment: RuntimeEnvironment
): TemporalConnector {
  const existing = environment.durableTransportById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof TemporalConnector)) {
      throw new Error(`durable transport ${String(connectorId)} is not Temporal`);
    }
    return existing;
  }
  const connector = new TemporalConnector(connectorId, environment);
  environment.addDurableTransport(connector);
  return connector;
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

function durableEnvelopeToWire(envelope: DurableEnvelope): DurableWorkflowRequest["envelope"] {
  return { ...envelope, payload: bytesToWire(envelope.payload) };
}

function durableEnvelopeFromWire(envelope: DurableWorkflowRequest["envelope"]): DurableEnvelope {
  return { ...envelope, payload: bytesFromWire(envelope.payload) };
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

function ownershipMemo(owner: string, callId: string): Record<string, unknown> {
  return { [MANAGED_BY]: "servicegen", [OWNER]: owner, [CALL_ID]: callId };
}

async function validateWorkflowOwnership(
  handle: WorkflowHandle,
  workflowType: string,
  owner: string,
  callId: string
): Promise<void> {
  const description = await handle.describe();
  if (description.type !== workflowType) throw new Error(`Temporal workflow ownership collision`);
  validateMemo(description.memo, owner, callId);
}

function validateMemo(
  memo: Record<string, unknown> | undefined,
  owner: string,
  callId: string
): void {
  if (memo?.[MANAGED_BY] !== "servicegen" || memo[OWNER] !== owner || memo[CALL_ID] !== callId) {
    throw new Error(`Temporal ownership collision for ${owner}`);
  }
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

function linkKey(link: DurableLinkId): string {
  return `${String(link.from)}:${String(link.to)}`;
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
    Promise.all(workerRuns).then(
      () => "stopped" as const,
      (error: unknown) => {
        throw error;
      }
    ),
    new Promise<"running">((resolve) => {
      setImmediate(resolve, "running");
    })
  ]);
  if (state === "stopped") {
    throw new Error("Temporal workers stopped during startup");
  }
}

function abortReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error("Temporal operation aborted");
}
