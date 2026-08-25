import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Client, ScheduleAlreadyRunning, ScheduleOverlapPolicy } from "@temporalio/client";
import { cancellationSignal, heartbeat } from "@temporalio/activity";
import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from "@temporalio/common";
import { NativeConnection, Runtime, Worker } from "@temporalio/worker";
import { DataConnectorType } from "../../runtime/config/index.js";
import { DurableCallContext, DurableCallEvent, bindDurableCallSpan, runDurableCallActivity } from "../../runtime/durable-call-context.js";
import { err, str, stringAttribute } from "../../runtime/environment/index.js";
import { normalizeTemporalPriority } from "../../runtime/schedule.js";
import { DURABLE_WORKFLOW_TYPE, ENDPOINT_WORKFLOW_TYPE } from "./contracts.js";
import { currentTemporalActivityMessageContext, runWithTemporalSubmissionContext, temporalActivityInterceptors, temporalWorkflowClientInterceptor } from "./context-propagation.js";
const MANAGED_BY = "servicelib.managedBy";
const OWNER = "servicelib.owner";
const CALL_ID = "servicelib.callId";
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
    #durableEvents;
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
        this.#durableEvents = environment
            .metrics()
            .scope("durable_call", { connector: this.name })
            .counterVec("events_total", "Total number of DurableCall Activity lifecycle events");
    }
    continuationActivityType() {
        return `${identityName(this.#environment.serviceConfig().name)}.durable_continuation.${identityName(this.name)}.v1`;
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
        const serviceName = this.#environment.serviceConfig().name;
        const source = this.#environment.runtimeConfig().streamById(link.from);
        const target = this.#environment.runtimeConfig().streamById(link.to);
        if (source === undefined || target === undefined) {
            throw new Error(`durable link ${key} references missing stream configuration`);
        }
        this.#links.set(key, {
            link,
            serviceName,
            sourceName: source.name,
            targetName: target.name,
            activityType: `${identityName(serviceName)}.durable.${identityName(source.name)}.${identityName(target.name)}.v1`,
            handler
        });
    }
    durableDiagnostics(boundary, target, context) {
        return (event, failure) => {
            this.#durableEvents.with({ boundary, target, event }).inc(context);
            if (failure === undefined)
                return;
            const fields = [
                str("connector", this.name),
                str("boundary", boundary),
                str("target", target),
                str("event", event),
                err(failure)
            ];
            if (event === DurableCallEvent.MissingOutcome ||
                event === DurableCallEvent.DuplicateTerminal ||
                event === DurableCallEvent.LateHeartbeat) {
                this.#environment.log().warn(context, "DurableCall Activity lifecycle misuse", ...fields);
            }
            else {
                this.#environment.log().error(context, "DurableCall Activity failed", ...fields);
            }
        };
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
    async submitLink(context, link, envelope) {
        if (!this.#started)
            throw new Error(`Temporal connector ${this.name} is not started`);
        const registration = this.#links.get(linkKey(link));
        if (registration === undefined)
            throw new Error(`durable link ${linkKey(link)} is not registered`);
        const policy = this.linkConfig(link);
        const request = {
            activityType: registration.activityType,
            continuationActivityType: this.continuationActivityType(),
            activityStartToCloseTimeout: policy.activityStartToCloseTimeout,
            activityHeartbeatTimeout: policy.activityHeartbeatTimeout,
            maximumAttempts: policy.maximumAttempts,
            priority: normalizeTemporalPriority(envelope.priority),
            envelope: durableEnvelopeToWire(envelope)
        };
        const owner = `${identityName(registration.serviceName)}/link/${identityName(registration.sourceName)}/${identityName(registration.targetName)}/v1`;
        const handle = await runWithTemporalSubmissionContext(context, () => this.client().workflow.start(DURABLE_WORKFLOW_TYPE, {
            args: [request],
            workflowId: `${identityName(registration.serviceName)}/durable/${identityName(registration.sourceName)}/${identityName(registration.targetName)}/${opaqueIdentityComponent(envelope.callId)}`,
            taskQueue: policy.taskQueue,
            ...(policy.workflowExecutionTimeout > 0
                ? { workflowExecutionTimeout: policy.workflowExecutionTimeout }
                : {}),
            workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
            workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
            memo: ownershipMemo(owner, envelope.callId),
            priority: { priorityKey: request.priority }
        }));
        await validateWorkflowOwnership(handle, DURABLE_WORKFLOW_TYPE, owner, envelope.callId);
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
        const request = endpointRequest(registration, config, envelope, this.continuationActivityType());
        const owner = `${identityName(this.name)}/endpoint/${identityName(config.name)}/v1`;
        const handle = await runWithTemporalSubmissionContext(context, () => this.client().workflow.start(ENDPOINT_WORKFLOW_TYPE, {
            args: [request],
            workflowId: `${identityName(this.name)}/endpoint/${identityName(config.name)}/${opaqueIdentityComponent(envelope.executionId)}`,
            taskQueue: config.taskQueue,
            ...(config.workflowExecutionTimeout > 0
                ? { workflowExecutionTimeout: config.workflowExecutionTimeout }
                : {}),
            workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
            workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
            memo: ownershipMemo(owner, envelope.executionId),
            priority: { priorityKey: request.priority }
        }));
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
            queue(this.linkConfig(registration.link).taskQueue)[registration.activityType] = async (value) => {
                const signal = cancellationSignal();
                const activityContext = currentTemporalActivityMessageContext();
                const envelope = durableEnvelopeFromWire(value);
                const durable = new DurableCallContext(envelope.callId, heartbeat, this.durableDiagnostics("link", `${String(registration.link.from)}:${String(registration.link.to)}`, activityContext));
                const result = await runDurableCallActivity(signal, durable, () => registration.handler(envelope, activityContext, signal, durable));
                return durableActivityResultToWire(result);
            };
        }
        for (const registration of this.#endpoints.values()) {
            const config = this.endpointConfig(registration.endpointId);
            if (!config.enabled || registration.handler === undefined)
                continue;
            const handler = registration.handler;
            queue(config.taskQueue)[registration.activityType] = async (value) => {
                const signal = cancellationSignal();
                const activityContext = currentTemporalActivityMessageContext();
                const envelope = endpointEnvelopeFromWire(value);
                if (!envelope.scheduled) {
                    const result = await handler(envelope, activityContext, signal);
                    return {
                        durable: {},
                        result: { payload: bytesToWire(result.payload) }
                    };
                }
                const durable = new DurableCallContext(envelope.executionId, heartbeat, this.durableDiagnostics("schedule", String(registration.endpointId), activityContext));
                let result = { payload: new Uint8Array() };
                const durableResult = await runDurableCallActivity(signal, durable, async () => {
                    result = await handler(envelope, activityContext, signal, durable);
                });
                return {
                    durable: durableActivityResultToWire(durableResult),
                    result: { payload: bytesToWire(result.payload) }
                };
            };
        }
        for (const activities of queues.values()) {
            activities[this.continuationActivityType()] = async (value) => {
                const continuation = durableContinuationFromWire(value);
                const signal = cancellationSignal();
                const activityContext = currentTemporalActivityMessageContext();
                const durable = new DurableCallContext(continuation.callId, heartbeat, this.durableDiagnostics("continuation", `${continuation.fromName}:${continuation.toName}`, activityContext));
                let context = activityContext
                    .withStreamId(continuation.streamId)
                    .withPriority(continuation.priority)
                    .withExternalCancellation(signal)
                    .withDurableCallContext(durable);
                if (continuation.deadlineUnixMillis > 0) {
                    context = context.bounded(Math.max(0, continuation.deadlineUnixMillis - Date.now()));
                }
                const result = await runDurableCallActivity(signal, durable, async () => {
                    const tracer = context.samplingEnabled()
                        ? this.#environment.tracing()?.tracer(this.#environment.serviceConfig().name)
                        : undefined;
                    if (tracer === undefined) {
                        await this.#environment.resumeDurableContinuation(context, continuation);
                        return;
                    }
                    const started = tracer.start(context, "temporal.activity", [
                        stringAttribute("boundary", "durable_delay"),
                        stringAttribute("from", continuation.fromName),
                        stringAttribute("to", continuation.toName)
                    ]);
                    const durableSpan = bindDurableCallSpan(started.context, started.span);
                    try {
                        await this.#environment.resumeDurableContinuation(started.context, continuation);
                    }
                    finally {
                        if (!durableSpan)
                            started.span.end();
                    }
                });
                return durableActivityResultToWire(result);
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
        const owner = `${identityName(this.name)}/endpoint/${identityName(config.name)}/v1`;
        const request = endpointRequest(registration, config, {
            version: 1,
            endpointId: config.id,
            executionId: "",
            streamId: "",
            priority: 0,
            deadlineUnixMillis: 0,
            scheduled: true,
            scheduleId: config.scheduleId,
            scheduledAtUnixMillis: 0,
            firedAtUnixMillis: 0,
            payload: new Uint8Array()
        }, this.continuationActivityType());
        try {
            await this.client().schedule.create({
                scheduleId: config.scheduleId,
                spec: { cronExpressions: [config.schedule], timezone: config.timezone },
                action: {
                    type: "startWorkflow",
                    workflowType: ENDPOINT_WORKFLOW_TYPE,
                    workflowId: `${this.name}/schedule/${config.name}`,
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
        return {
            endpointId,
            activityType: `${identityName(this.name)}.endpoint.${identityName(config.name)}.v1`
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
function endpointRequest(registration, config, envelope, continuationActivityType) {
    return {
        activityType: registration.activityType,
        continuationActivityType,
        activityStartToCloseTimeout: config.activityStartToCloseTimeout,
        activityHeartbeatTimeout: config.activityHeartbeatTimeout,
        maximumAttempts: config.maximumAttempts,
        priority: normalizeTemporalPriority(envelope.priority),
        envelope: endpointEnvelopeToWire(envelope)
    };
}
function durableActivityResultToWire(result) {
    return result.continuation === undefined
        ? {}
        : { continuation: durableContinuationToWire(result.continuation) };
}
function durableContinuationToWire(continuation) {
    return { ...continuation, payload: bytesToWire(continuation.payload) };
}
function durableContinuationFromWire(value) {
    if (typeof value !== "object" || value === null) {
        throw new TypeError("invalid Temporal durable continuation");
    }
    const continuation = value;
    if (continuation["version"] !== 1 ||
        typeof continuation["fromName"] !== "string" ||
        continuation["fromName"] === "" ||
        typeof continuation["toName"] !== "string" ||
        continuation["toName"] === "" ||
        typeof continuation["callId"] !== "string" ||
        continuation["callId"] === "" ||
        typeof continuation["streamId"] !== "string" ||
        typeof continuation["priority"] !== "number" ||
        typeof continuation["deadlineUnixMillis"] !== "number" ||
        typeof continuation["wakeAtUnixMillis"] !== "number") {
        throw new TypeError("invalid Temporal durable continuation");
    }
    return {
        version: 1,
        fromName: continuation["fromName"],
        toName: continuation["toName"],
        callId: continuation["callId"],
        streamId: continuation["streamId"],
        priority: continuation["priority"],
        deadlineUnixMillis: continuation["deadlineUnixMillis"],
        wakeAtUnixMillis: continuation["wakeAtUnixMillis"],
        payload: bytesFromWire(continuation["payload"])
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
    return { [MANAGED_BY]: "servicelib", [OWNER]: owner, [CALL_ID]: callId };
}
function opaqueIdentityComponent(value) {
    return encodeURIComponent(value).replaceAll(/[!'()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
}
// Intentionally identical to servicegen.splitWords + ToSnakeCase.
function identityName(value) {
    const words = [];
    let current = [];
    const characters = Array.from(value);
    for (const [index, character] of characters.entries()) {
        if (/\s/u.test(character) || "_-/.".includes(character)) {
            if (current.length > 0) {
                words.push(current.join(""));
                current = [];
            }
            continue;
        }
        if (!/[\p{L}\p{N}]/u.test(character))
            continue;
        const upper = character.toUpperCase() === character && character.toLowerCase() !== character;
        if (current.length > 0 && upper) {
            const previous = current.at(-1) ?? "";
            const previousUpper = previous.toUpperCase() === previous && previous.toLowerCase() !== previous;
            const next = characters[index + 1];
            const nextLower = next?.toLowerCase() === next && next?.toUpperCase() !== next;
            if (!previousUpper || nextLower) {
                words.push(current.join(""));
                current = [];
            }
        }
        current.push(character);
    }
    if (current.length > 0)
        words.push(current.join(""));
    return words.map((word) => word.toLowerCase()).join("_");
}
async function validateWorkflowOwnership(handle, workflowType, owner, callId) {
    const description = await handle.describe();
    if (description.type !== workflowType)
        throw new Error(`Temporal workflow ownership collision`);
    validateMemo(description.memo, owner, callId);
}
function validateMemo(memo, owner, callId) {
    if (memo?.[MANAGED_BY] !== "servicelib" || memo[OWNER] !== owner || memo[CALL_ID] !== callId) {
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