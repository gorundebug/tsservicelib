import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Client, ScheduleAlreadyRunning, ScheduleOverlapPolicy } from "@temporalio/client";
import { cancellationSignal, heartbeat } from "@temporalio/activity";
import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from "@temporalio/common";
import { NativeConnection, Runtime, Worker } from "@temporalio/worker";
import { DataConnectorType } from "../../runtime/config/index.js";
import { DurableCallContext, DurableCallEvent, runDurableCallActivity } from "../../runtime/durable-call-context.js";
import { err, str } from "../../runtime/environment/index.js";
import { normalizeTemporalPriority } from "../../runtime/schedule.js";
import { ENDPOINT_WORKFLOW_TYPE, temporalDirectWorkflowType, temporalEndpointActivityType, temporalEndpointWorkflowId, temporalIdentityName } from "./contracts.js";
import { currentTemporalActivityMessageContext, runWithTemporalSubmissionContext, temporalActivityInterceptors, temporalWorkflowClientInterceptor } from "./context-propagation.js";
const MANAGED_BY = "servicelib.managedBy";
const OWNER = "servicelib.owner";
const CALL_ID = "servicelib.callId";
const SDK_METRICS_BIND_ADDRESS_ENVIRONMENT = "TEMPORAL_SDK_METRICS_BIND_ADDRESS";
let sdkMetricsBindAddress;
export function temporalCronExpression(expression) {
    return `0 ${expression.trim().split(/\s+/u).join(" ")}`;
}
/**
 * Workflow interceptor modules shared by live Workers and offline history
 * replayers. Keeping this list in one place prevents replay from silently
 * using different native-header propagation semantics.
 */
export function temporalWorkflowInterceptorModules() {
    return [fileURLToPath(new URL("./workflow-context-interceptor.js", import.meta.url))];
}
/** Replay one Temporal Event History with the live Worker's Workflow bundle. */
export async function replayTemporalWorkflowHistory(workflowsPath, history, workflowId = "servicegen-replay") {
    await Worker.runReplayHistory({
        workflowsPath,
        interceptors: { workflowModules: temporalWorkflowInterceptorModules() }
    }, history, workflowId);
}
export class TemporalConnector {
    #environment;
    #endpoints = new Map();
    #activityEvents;
    #telemetryPlugin;
    #telemetryClientInterceptor;
    #connection;
    #client;
    #workers = [];
    #workerRuns = [];
    #started = false;
    #admissionStarted = false;
    #workflowsPath;
    id;
    name;
    constructor(connectorId, environment, options = {}) {
        const config = environment.runtimeConfig().dataConnectorById(connectorId);
        if (config?.type !== DataConnectorType.Temporal) {
            throw new Error(`data connector ${String(connectorId)} is not Temporal`);
        }
        this.id = connectorId;
        this.name = config.name;
        this.#environment = environment;
        this.#workflowsPath =
            options.workflowsPath ?? fileURLToPath(new URL("./workflows.js", import.meta.url));
        this.#activityEvents = environment
            .metrics()
            .scope("temporal_activity", { connector: this.name })
            .counterVec("events_total", "Total number of Temporal Activity lifecycle events");
        const tracing = environment.tracing();
        this.#telemetryPlugin = temporalWorkerPlugin(tracing);
        this.#telemetryClientInterceptor = getTemporalWorkflowClientInterceptor(tracing);
    }
    registerEndpoint(endpointId, handler) {
        if (this.#started)
            throw new Error("cannot register endpoint after Temporal start");
        const registration = this.#endpoints.get(endpointId) ?? this.endpointRegistration(endpointId);
        if (registration.handler !== undefined) {
            throw new Error(`Temporal endpoint ${String(endpointId)} is already registered`);
        }
        this.#endpoints.set(endpointId, { ...registration, handler });
    }
    assertOptions(options) {
        if (options.workflowsPath !== undefined && options.workflowsPath !== this.#workflowsPath) {
            throw new Error(`Temporal connector ${this.name} was initialized with another Workflow bundle`);
        }
    }
    registerEndpointSubmission(endpointId) {
        if (!this.#endpoints.has(endpointId)) {
            this.#endpoints.set(endpointId, this.endpointRegistration(endpointId));
        }
    }
    async start(context) {
        if (this.#started)
            return;
        context.signal().throwIfAborted();
        const config = this.config();
        resolveWorkerStopTimeout(config.workerStopTimeout, this.#environment.serviceConfig().shutdownTimeout);
        installSdkMetricsRuntime();
        const tls = await tlsOptions(config);
        const connection = await abortable(NativeConnection.connect({
            address: config.address,
            ...(tls === undefined ? {} : { tls }),
            ...(config.apiKey === "" ? {} : { apiKey: config.apiKey }),
            ...(context.remainingMs() === undefined ? {} : { connectTimeout: context.remainingMs() })
        }), context.signal());
        this.#connection = connection;
        this.#client = new Client({
            connection,
            namespace: config.namespace,
            interceptors: {
                workflow: [
                    temporalWorkflowClientInterceptor,
                    ...(this.#telemetryClientInterceptor === undefined
                        ? []
                        : [this.#telemetryClientInterceptor])
                ]
            },
            ...(config.identity === "" ? {} : { identity: config.identity })
        });
        try {
            this.#started = true;
            for (const endpointId of this.#endpoints.keys()) {
                const endpoint = this.endpointConfig(endpointId);
                if (endpoint.enabled && endpoint.schedule !== "")
                    await this.ensureSchedule(endpoint);
            }
        }
        catch (error) {
            try {
                await this.shutdownWorkers();
            }
            catch (shutdownError) {
                this.#environment
                    .log()
                    .warn(context, "Temporal Worker startup rollback failed", str("connector", this.name), err(shutdownError instanceof Error ? shutdownError : new Error(String(shutdownError))));
            }
            try {
                await connection.close();
            }
            catch (closeError) {
                this.#environment
                    .log()
                    .warn(context, "Temporal connection startup rollback failed", str("connector", this.name), err(closeError instanceof Error ? closeError : new Error(String(closeError))));
            }
            this.#connection = undefined;
            this.#client = undefined;
            throw error;
        }
    }
    async startAdmission(context) {
        if (!this.#started || this.#connection === undefined) {
            throw new Error(`Temporal connector ${this.name} is not started`);
        }
        if (this.#admissionStarted)
            return;
        context.signal().throwIfAborted();
        const config = this.config();
        const workerStopTimeout = resolveWorkerStopTimeout(config.workerStopTimeout, this.#environment.serviceConfig().shutdownTimeout);
        try {
            for (const [taskQueue, activities] of this.queueActivities()) {
                const policy = this.queuePolicy(taskQueue);
                const worker = await Worker.create({
                    connection: this.#connection,
                    namespace: config.namespace,
                    taskQueue,
                    activities,
                    workflowsPath: this.#workflowsPath,
                    interceptors: {
                        activity: [temporalActivityInterceptors],
                        workflowModules: temporalWorkflowInterceptorModules()
                    },
                    ...(this.#telemetryPlugin === undefined ? {} : { plugins: [this.#telemetryPlugin] }),
                    ...(config.identity === "" ? {} : { identity: config.identity }),
                    ...(policy.maxConcurrentActivities > 0
                        ? { maxConcurrentActivityTaskExecutions: policy.maxConcurrentActivities }
                        : {}),
                    ...(policy.maxConcurrentWorkflowTasks > 0
                        ? { maxConcurrentWorkflowTaskExecutions: policy.maxConcurrentWorkflowTasks }
                        : {}),
                    shutdownGraceTime: workerStopTimeout
                });
                this.#workers.push(worker);
                this.#workerRuns.push(worker.run());
            }
            await ensureWorkersRunning(this.#workerRuns);
            this.#admissionStarted = true;
        }
        catch (error) {
            await this.shutdownWorkers();
            throw error;
        }
    }
    async stopAdmission(context) {
        void context;
        await this.shutdownWorkers();
        this.#admissionStarted = false;
    }
    async stop(context) {
        let shutdownError;
        try {
            await this.shutdownWorkers();
        }
        catch (error) {
            shutdownError = error;
        }
        const connection = this.#connection;
        this.#started = false;
        this.#admissionStarted = false;
        this.#client = undefined;
        this.#connection = undefined;
        try {
            if (connection !== undefined)
                await connection.close();
        }
        catch (closeError) {
            if (shutdownError === undefined)
                shutdownError = closeError;
            else {
                this.#environment
                    .log()
                    .warn(context, "Temporal connection shutdown failed after Worker failure", str("connector", this.name), err(closeError instanceof Error ? closeError : new Error(String(closeError))));
            }
        }
        if (shutdownError !== undefined) {
            throw shutdownError instanceof Error
                ? shutdownError
                : new Error("Temporal connector shutdown failed", { cause: shutdownError });
        }
    }
    async submitEndpoint(context, endpointId, envelope, waitForResult) {
        if (!this.#started)
            throw new Error(`Temporal connector ${this.name} is not started`);
        const registration = this.#endpoints.get(endpointId);
        if (registration === undefined) {
            throw new Error(`Temporal endpoint ${String(endpointId)} is not registered`);
        }
        const config = this.endpointConfig(endpointId);
        if (!config.enabled)
            throw new Error(`Temporal endpoint ${config.name} is disabled`);
        if (envelope.messageId === "")
            throw new Error("Temporal endpoint message ID is empty");
        const request = endpointRequest(registration, config, envelope, this.#environment.runtimeConfig().config());
        const workflowType = config.temporalExecutionType === "Workflow"
            ? registration.workflowType
            : ENDPOINT_WORKFLOW_TYPE;
        const owner = endpointOwner(this.name, config.name);
        const handle = await runWithTemporalSubmissionContext(context, () => this.client().workflow.start(workflowType, {
            args: [request],
            workflowId: temporalEndpointWorkflowId(this.name, config.name, envelope.messageId),
            taskQueue: config.taskQueue,
            ...(config.workflowExecutionTimeout > 0
                ? { workflowExecutionTimeout: config.workflowExecutionTimeout }
                : {}),
            workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
            workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
            memo: ownershipMemo(owner, envelope.messageId),
            priority: { priorityKey: request.priority }
        }));
        await validateWorkflowOwnership(handle, workflowType, owner, envelope.messageId);
        if (!waitForResult)
            return { payload: new Uint8Array() };
        const result = (await handle.result());
        return { payload: bytesFromWire(result.payload) };
    }
    queueActivities() {
        const queues = new Map();
        for (const registration of this.#endpoints.values()) {
            const config = this.endpointConfig(registration.endpointId);
            if (!config.enabled || registration.handler === undefined)
                continue;
            if (!queues.has(config.taskQueue))
                queues.set(config.taskQueue, {});
            if (config.temporalExecutionType !== "Activity")
                continue;
            const handler = registration.handler;
            const activities = queues.get(config.taskQueue) ?? {};
            queues.set(config.taskQueue, activities);
            activities[registration.activityType] = async (value) => {
                const signal = cancellationSignal();
                const envelope = endpointEnvelopeFromWire(value);
                const durable = new DurableCallContext(envelope.messageId, "Activity", {
                    heartbeat,
                    diagnostics: this.activityDiagnostics("endpoint", String(registration.endpointId), currentTemporalActivityMessageContext())
                });
                const context = currentTemporalActivityMessageContext()
                    .withExternalCancellation(signal)
                    .withDurableCallContext(durable);
                const result = await runDurableCallActivity(durable, () => handler(envelope, context, signal));
                return { payload: bytesToWire(result.payload) };
            };
        }
        return queues;
    }
    queuePolicy(taskQueue) {
        let maxConcurrentActivities = 0;
        let maxConcurrentWorkflowTasks = 0;
        for (const registration of this.#endpoints.values()) {
            const config = this.endpointConfig(registration.endpointId);
            if (!config.enabled || config.taskQueue !== taskQueue || registration.handler === undefined)
                continue;
            if (config.temporalExecutionType === "Activity") {
                if (maxConcurrentActivities !== 0 &&
                    maxConcurrentActivities !== config.maxConcurrentActivities) {
                    throw new Error(`Temporal Task Queue ${taskQueue} has conflicting maxConcurrentActivities`);
                }
                maxConcurrentActivities = config.maxConcurrentActivities;
            }
            else {
                if (maxConcurrentWorkflowTasks !== 0 &&
                    maxConcurrentWorkflowTasks !== config.maxConcurrentWorkflowTasks) {
                    throw new Error(`Temporal Task Queue ${taskQueue} has conflicting maxConcurrentWorkflowTasks`);
                }
                maxConcurrentWorkflowTasks = config.maxConcurrentWorkflowTasks;
            }
        }
        return { maxConcurrentActivities, maxConcurrentWorkflowTasks };
    }
    activityDiagnostics(boundary, target, context) {
        return (event, failure) => {
            this.#activityEvents.with({ connector: this.name, boundary, target, event }).inc(context);
            if (failure === undefined)
                return;
            const fields = [
                str("connector", this.name),
                str("boundary", boundary),
                str("target", target),
                str("event", event),
                err(failure)
            ];
            if (event === DurableCallEvent.LateHeartbeat) {
                this.#environment.log().warn(context, "Temporal Activity lifecycle misuse", ...fields);
            }
            else {
                this.#environment.log().error(context, "Temporal Activity failed", ...fields);
            }
        };
    }
    async ensureSchedule(config) {
        const registration = this.#endpoints.get(config.id);
        if (registration?.handler === undefined)
            return;
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
        }, this.#environment.runtimeConfig().config());
        const workflowType = config.temporalExecutionType === "Workflow"
            ? registration.workflowType
            : ENDPOINT_WORKFLOW_TYPE;
        try {
            await this.client().schedule.create({
                scheduleId: config.scheduleId,
                spec: {
                    cronExpressions: [temporalCronExpression(config.schedule)],
                    timezone: config.timezone
                },
                action: {
                    type: "startWorkflow",
                    workflowType,
                    workflowId: `${temporalIdentityName(this.name)}/schedule/${temporalIdentityName(config.name)}`,
                    taskQueue: config.taskQueue,
                    args: [request],
                    memo: ownershipMemo(owner, config.scheduleId),
                    ...(config.workflowExecutionTimeout > 0
                        ? { workflowExecutionTimeout: config.workflowExecutionTimeout }
                        : {})
                },
                policies: {
                    overlap: config.overlapPolicy === "Allow"
                        ? ScheduleOverlapPolicy.ALLOW_ALL
                        : ScheduleOverlapPolicy.SKIP,
                    catchupWindow: config.missedRunPolicy === "FireOnce" ? 365 * 24 * 60 * 60 * 1000 : 10_000
                },
                memo: ownershipMemo(owner, config.scheduleId)
            });
        }
        catch (error) {
            if (!(error instanceof ScheduleAlreadyRunning))
                throw error;
            const description = await this.client().schedule.getHandle(config.scheduleId).describe();
            validateMemo(description.memo, owner, config.scheduleId);
            if (description.action.workflowType !== workflowType ||
                description.action.taskQueue !== config.taskQueue) {
                throw new Error(`Temporal schedule ${config.scheduleId} ownership collision`, {
                    cause: error
                });
            }
        }
    }
    endpointRegistration(endpointId) {
        const config = this.endpointConfig(endpointId);
        if (config.idDataConnector !== this.id) {
            throw new Error(`endpoint ${String(endpointId)} does not belong to connector ${this.name}`);
        }
        return {
            endpointId,
            activityType: temporalEndpointActivityType(this.name, config.name),
            workflowType: temporalDirectWorkflowType(this.name, config.name)
        };
    }
    config() {
        const config = this.#environment.runtimeConfig().dataConnectorById(this.id);
        if (!isTemporalConnectorConfig(config)) {
            throw new Error(`Temporal connector ${this.name} configuration not found`);
        }
        return config;
    }
    endpointConfig(endpointId) {
        const config = this.#environment.runtimeConfig().endpointById(endpointId);
        if (!isTemporalEndpointConfig(config)) {
            throw new Error(`Temporal endpoint configuration ${String(endpointId)} not found`);
        }
        return config;
    }
    client() {
        if (this.#client === undefined)
            throw new Error(`Temporal connector ${this.name} is not started`);
        return this.#client;
    }
    async shutdownWorkers() {
        const shutdownResults = await Promise.allSettled(this.#workers.map((worker) => Promise.resolve().then(() => {
            worker.shutdown();
        })));
        const runResults = await Promise.allSettled(this.#workerRuns);
        this.#workers = [];
        this.#workerRuns = [];
        const failure = [...shutdownResults, ...runResults].find((result) => result.status === "rejected");
        if (failure !== undefined) {
            throw failure.reason instanceof Error
                ? failure.reason
                : new Error("Temporal worker shutdown failed", { cause: failure.reason });
        }
    }
}
export function makeTemporalConnector(connectorId, environment, options = {}) {
    const existing = environment.managedDataConnectorById(connectorId);
    if (existing !== undefined) {
        if (!(existing instanceof TemporalConnector)) {
            throw new Error(`managed connector ${String(connectorId)} is not Temporal`);
        }
        existing.assertOptions(options);
        return existing;
    }
    const connector = new TemporalConnector(connectorId, environment, options);
    environment.addManagedDataConnector(connector);
    return connector;
}
export function endpointWorkflowId(connectorName, endpointName, messageId) {
    return temporalEndpointWorkflowId(connectorName, endpointName, messageId);
}
function endpointOwner(connectorName, endpointName) {
    return `${temporalIdentityName(connectorName)}/endpoint/${temporalIdentityName(endpointName)}/v1`;
}
function endpointRequest(registration, config, envelope, runtimeConfig) {
    return {
        executionType: config.temporalExecutionType,
        runtimeConfig,
        activityType: registration.activityType,
        activityStartToCloseTimeout: config.activityStartToCloseTimeout,
        activityHeartbeatTimeout: config.activityHeartbeatTimeout,
        maximumAttempts: config.maximumAttempts,
        priority: normalizeTemporalPriority(envelope.priority),
        envelope: endpointEnvelopeToWire(envelope)
    };
}
function endpointEnvelopeToWire(envelope) {
    return { ...envelope, payload: bytesToWire(envelope.payload) };
}
function endpointEnvelopeFromWire(envelope) {
    return { ...envelope, payload: bytesFromWire(envelope.payload) };
}
function bytesToWire(value) {
    return Array.from(value);
}
function bytesFromWire(value) {
    if (!Array.isArray(value) ||
        value.some((item) => !Number.isInteger(item) || item < 0 || item > 255)) {
        throw new TypeError("invalid Temporal byte payload");
    }
    return Uint8Array.from(value);
}
function ownershipMemo(owner, messageId) {
    return { [MANAGED_BY]: "servicelib", [OWNER]: owner, [CALL_ID]: messageId };
}
async function validateWorkflowOwnership(handle, workflowType, owner, messageId) {
    const description = await handle.describe();
    if (description.type !== workflowType)
        throw new Error("Temporal workflow ownership collision");
    validateMemo(description.memo, owner, messageId);
}
function validateMemo(memo, owner, messageId) {
    if (memo?.[MANAGED_BY] !== "servicelib" || memo[OWNER] !== owner || memo[CALL_ID] !== messageId) {
        throw new Error(`Temporal ownership collision for ${owner}`);
    }
}
function isTemporalConnectorConfig(config) {
    return (config?.type === DataConnectorType.Temporal && "address" in config && "namespace" in config);
}
function isTemporalEndpointConfig(config) {
    return config !== undefined && "taskQueue" in config && typeof config.taskQueue === "string";
}
function installSdkMetricsRuntime() {
    const address = process.env[SDK_METRICS_BIND_ADDRESS_ENVIRONMENT]?.trim();
    if (address === undefined || address === "")
        return;
    if (sdkMetricsBindAddress !== undefined) {
        if (sdkMetricsBindAddress !== address) {
            throw new Error(`Temporal SDK metrics already listen on ${sdkMetricsBindAddress}, cannot also use ${address}`);
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
function temporalTracingIntegration(tracing) {
    if (tracing === undefined ||
        !("temporalWorkerPlugin" in tracing) ||
        !("temporalWorkflowClientInterceptor" in tracing) ||
        typeof tracing.temporalWorkerPlugin !== "function" ||
        typeof tracing.temporalWorkflowClientInterceptor !== "function") {
        return undefined;
    }
    return tracing;
}
function temporalWorkerPlugin(tracing) {
    return temporalTracingIntegration(tracing)?.temporalWorkerPlugin();
}
function getTemporalWorkflowClientInterceptor(tracing) {
    return temporalTracingIntegration(tracing)?.temporalWorkflowClientInterceptor();
}
async function tlsOptions(config) {
    if (!config.tlsEnabled)
        return undefined;
    if ((config.tlsCertFile === "") !== (config.tlsKeyFile === "")) {
        throw new Error(`Temporal connector ${config.name} requires both TLS cert and key`);
    }
    const ca = config.tlsCaFile === "" ? undefined : await readFile(config.tlsCaFile);
    const pair = config.tlsCertFile === ""
        ? undefined
        : {
            crt: await readFile(config.tlsCertFile),
            key: await readFile(config.tlsKeyFile)
        };
    if (config.tlsServerName === "" && ca === undefined && pair === undefined)
        return true;
    return {
        ...(config.tlsServerName === "" ? {} : { serverNameOverride: config.tlsServerName }),
        ...(ca === undefined ? {} : { serverRootCACertificate: ca }),
        ...(pair === undefined ? {} : { clientCertPair: pair })
    };
}
function resolveWorkerStopTimeout(configuredMillis, serviceShutdownMillis) {
    if (!Number.isSafeInteger(configuredMillis) || configuredMillis < 0) {
        throw new Error("workerStopTimeout must be a non-negative integer");
    }
    if (!Number.isSafeInteger(serviceShutdownMillis) || serviceShutdownMillis < 0) {
        throw new Error("service shutdownTimeout must be a non-negative integer");
    }
    const timeout = configuredMillis === 0 ? serviceShutdownMillis : configuredMillis;
    if (timeout > serviceShutdownMillis) {
        throw new Error(`workerStopTimeout ${String(timeout)}ms exceeds service shutdownTimeout ${String(serviceShutdownMillis)}ms`);
    }
    return timeout;
}
async function abortable(promise, signal) {
    if (signal.aborted)
        throw abortReason(signal);
    return new Promise((resolve, reject) => {
        const onAbort = () => {
            reject(abortReason(signal));
        };
        signal.addEventListener("abort", onAbort, { once: true });
        void promise.then((value) => {
            signal.removeEventListener("abort", onAbort);
            resolve(value);
        }, (error) => {
            signal.removeEventListener("abort", onAbort);
            reject(error instanceof Error ? error : new Error(String(error)));
        });
    });
}
async function ensureWorkersRunning(workerRuns) {
    if (workerRuns.length === 0)
        return;
    const state = await Promise.race([
        Promise.all(workerRuns).then(() => "stopped"),
        new Promise((resolve) => {
            setImmediate(resolve, "running");
        })
    ]);
    if (state === "stopped")
        throw new Error("Temporal workers stopped during startup");
}
function abortReason(signal) {
    return signal.reason instanceof Error ? signal.reason : new Error("Temporal operation aborted");
}
//# sourceMappingURL=connector.js.map