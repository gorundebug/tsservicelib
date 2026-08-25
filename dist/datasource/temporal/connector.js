import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Client, ScheduleAlreadyRunning, ScheduleOverlapPolicy } from "@temporalio/client";
import { cancellationSignal } from "@temporalio/activity";
import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from "@temporalio/common";
import { NativeConnection, Runtime, Worker } from "@temporalio/worker";
import { DataConnectorType } from "../../runtime/config/index.js";
import { normalizeTemporalPriority } from "../../runtime/schedule.js";
import { DURABLE_WORKFLOW_TYPE, ENDPOINT_WORKFLOW_TYPE } from "./contracts.js";
const MANAGED_BY = "servicegen.managedBy";
const OWNER = "servicegen.owner";
const CALL_ID = "servicegen.callId";
const SDK_METRICS_BIND_ADDRESS_ENVIRONMENT = "TEMPORAL_SDK_METRICS_BIND_ADDRESS";
let sdkMetricsBindAddress;
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
export class TemporalConnector {
    #environment;
    #links = new Map();
    #endpoints = new Map();
    #connection;
    #client;
    #workers = [];
    #workerRuns = [];
    #started = false;
    id;
    name;
    constructor(connectorId, environment) {
        const config = environment.runtimeConfig().dataConnectorById(connectorId);
        if (config?.type !== DataConnectorType.Temporal) {
            throw new Error(`data connector ${String(connectorId)} is not Temporal`);
        }
        this.id = connectorId;
        this.name = config.name;
        this.#environment = environment;
    }
    registerLink(link, handler) {
        if (this.#started)
            throw new Error("cannot register DurableCall after Temporal start");
        const key = linkKey(link);
        if (this.#links.has(key))
            throw new Error(`durable link ${key} is already registered`);
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
    registerEndpoint(endpointId, handler) {
        if (this.#started)
            throw new Error("cannot register endpoint after Temporal start");
        const registration = this.#endpoints.get(endpointId) ?? this.endpointRegistration(endpointId);
        if (registration.handler !== undefined) {
            throw new Error(`Temporal endpoint ${String(endpointId)} is already registered`);
        }
        this.#endpoints.set(endpointId, { ...registration, handler });
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
                if (endpoint.enabled && endpoint.schedule !== "")
                    await this.ensureSchedule(endpoint);
            }
            this.#started = true;
        }
        catch (error) {
            await this.shutdownWorkers();
            await connection.close();
            this.#connection = undefined;
            this.#client = undefined;
            throw error;
        }
    }
    async stopAdmission(_context) {
        void _context;
        await this.shutdownWorkers();
    }
    async stop(_context) {
        void _context;
        await this.shutdownWorkers();
        this.#started = false;
        const connection = this.#connection;
        this.#client = undefined;
        this.#connection = undefined;
        if (connection !== undefined)
            await connection.close();
    }
    async submitLink(link, envelope) {
        if (!this.#started)
            throw new Error(`Temporal connector ${this.name} is not started`);
        const registration = this.#links.get(linkKey(link));
        if (registration === undefined)
            throw new Error(`durable link ${linkKey(link)} is not registered`);
        const policy = this.linkConfig(link);
        const request = {
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
    async submitEndpoint(endpointId, envelope, waitForResult) {
        if (!this.#started)
            throw new Error(`Temporal connector ${this.name} is not started`);
        const registration = this.#endpoints.get(endpointId);
        if (registration === undefined) {
            throw new Error(`Temporal endpoint ${String(endpointId)} is not registered`);
        }
        const config = this.endpointConfig(endpointId);
        if (!config.enabled)
            throw new Error(`Temporal endpoint ${config.name} is disabled`);
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
        if (!waitForResult)
            return { payload: new Uint8Array() };
        const result = (await handle.result());
        return { payload: bytesFromWire(result.payload) };
    }
    async connect(config, context) {
        installSdkMetricsRuntime();
        const tls = await tlsOptions(config);
        const connect = NativeConnection.connect({
            address: config.address,
            ...(tls === undefined ? {} : { tls }),
            ...(config.apiKey === "" ? {} : { apiKey: config.apiKey }),
            ...(context.remainingMs() === undefined ? {} : { connectTimeout: context.remainingMs() })
        });
        return abortable(connect, context.signal());
    }
    queueActivities() {
        const queues = new Map();
        const queue = (name) => {
            const existing = queues.get(name);
            if (existing !== undefined)
                return existing;
            const created = {};
            queues.set(name, created);
            return created;
        };
        for (const registration of this.#links.values()) {
            queue(this.linkConfig(registration.link).taskQueue)[registration.activityType] = async (value) => registration.handler(durableEnvelopeFromWire(value), cancellationSignal());
        }
        for (const registration of this.#endpoints.values()) {
            const config = this.endpointConfig(registration.endpointId);
            if (!config.enabled || registration.handler === undefined)
                continue;
            const handler = registration.handler;
            queue(config.taskQueue)[registration.activityType] = async (value) => {
                const result = await handler(endpointEnvelopeFromWire(value), cancellationSignal());
                return { payload: bytesToWire(result.payload) };
            };
        }
        return queues;
    }
    async shutdownWorkers() {
        for (const worker of this.#workers)
            worker.shutdown();
        await Promise.allSettled(this.#workerRuns);
        this.#workers = [];
        this.#workerRuns = [];
    }
    async ensureSchedule(config) {
        const registration = this.#endpoints.get(config.id);
        if (registration?.handler === undefined)
            return;
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
            traceCarrier: {},
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
            if (description.action.workflowType !== ENDPOINT_WORKFLOW_TYPE ||
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
        const inputs = this.#environment
            .runtimeConfig()
            .config()
            .streams.filter((stream) => isInputStreamConfig(stream) && stream.idEndpoint === endpointId);
        if (inputs.length !== 1) {
            throw new Error(`Temporal endpoint ${String(endpointId)} must have exactly one input stream; found ${String(inputs.length)}`);
        }
        return {
            endpointId,
            activityType: `servicegen.endpoint.${String(inputs[0]?.idService)}.${String(endpointId)}.v1`
        };
    }
    config() {
        const config = this.#environment.runtimeConfig().dataConnectorById(this.id);
        if (!isTemporalConnectorConfig(config)) {
            throw new Error(`Temporal connector ${this.name} configuration not found`);
        }
        return config;
    }
    linkConfig(link) {
        const semantics = this.#environment.runtimeConfig().link(link.from, link.to)?.callSemantics;
        if (semantics === undefined || !("durableCall" in semantics)) {
            throw new Error(`DurableCall configuration ${linkKey(link)} not found`);
        }
        return semantics.durableCall;
    }
    endpointConfig(endpointId) {
        const config = this.#environment.runtimeConfig().endpointById(endpointId);
        if (!isTemporalEndpointConfig(config)) {
            throw new Error(`Temporal endpoint configuration ${String(endpointId)} not found`);
        }
        return config;
    }
    client() {
        if (this.#client === undefined) {
            throw new Error(`Temporal connector ${this.name} is not started`);
        }
        return this.#client;
    }
}
function isTemporalConnectorConfig(config) {
    return (config?.type === DataConnectorType.Temporal && "address" in config && "namespace" in config);
}
function isTemporalEndpointConfig(config) {
    return config !== undefined && "taskQueue" in config && typeof config.taskQueue === "string";
}
function isInputStreamConfig(config) {
    return config.type === "Input" && "idEndpoint" in config;
}
export function makeTemporalConnector(connectorId, environment) {
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
function endpointRequest(registration, config, envelope) {
    return {
        activityType: registration.activityType,
        activityStartToCloseTimeout: config.activityStartToCloseTimeout,
        activityHeartbeatTimeout: config.activityHeartbeatTimeout,
        maximumAttempts: config.maximumAttempts,
        priority: normalizeTemporalPriority(envelope.priority),
        envelope: endpointEnvelopeToWire(envelope)
    };
}
function durableEnvelopeToWire(envelope) {
    return { ...envelope, payload: bytesToWire(envelope.payload) };
}
function durableEnvelopeFromWire(envelope) {
    return { ...envelope, payload: bytesFromWire(envelope.payload) };
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
function ownershipMemo(owner, callId) {
    return { [MANAGED_BY]: "servicegen", [OWNER]: owner, [CALL_ID]: callId };
}
async function validateWorkflowOwnership(handle, workflowType, owner, callId) {
    const description = await handle.describe();
    if (description.type !== workflowType)
        throw new Error(`Temporal workflow ownership collision`);
    validateMemo(description.memo, owner, callId);
}
function validateMemo(memo, owner, callId) {
    if (memo?.[MANAGED_BY] !== "servicegen" || memo[OWNER] !== owner || memo[CALL_ID] !== callId) {
        throw new Error(`Temporal ownership collision for ${owner}`);
    }
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
function linkKey(link) {
    return `${String(link.from)}:${String(link.to)}`;
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
        Promise.all(workerRuns).then(() => "stopped", (error) => {
            throw error;
        }),
        new Promise((resolve) => {
            setImmediate(resolve, "running");
        })
    ]);
    if (state === "stopped") {
        throw new Error("Temporal workers stopped during startup");
    }
}
function abortReason(signal) {
    return signal.reason instanceof Error ? signal.reason : new Error("Temporal operation aborted");
}
//# sourceMappingURL=connector.js.map