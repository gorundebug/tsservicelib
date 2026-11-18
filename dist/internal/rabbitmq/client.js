import { Worker } from "node:worker_threads";
import { serialize, deserialize } from "node:v8";
import { Context } from "../../runtime/index.js";
import { wait } from "./wait.js";
import { BYTE_LIMIT, MESSAGE_LIMIT } from "./protocol.js";
export { wait, cancellation } from "./wait.js";
export { BYTE_LIMIT, MESSAGE_LIMIT } from "./protocol.js";
export const PENDING_MESSAGES = 256;
export class Session {
    config;
    ep;
    source;
    report;
    receive;
    worker;
    ready = false;
    epoch = 0;
    stopped = false;
    paused = false;
    limit;
    #pending = new Map();
    #serial = 0;
    #attempts = 0;
    #recovering;
    #controller = new AbortController();
    #closing;
    #removeOwner;
    constructor(config, ep, source, report, receive = () => undefined) {
        this.config = config;
        this.ep = ep;
        this.source = source;
        this.report = report;
        this.receive = receive;
        this.limit = (source ? ep.consumerCredits : ep.publisherMaxInFlight) ?? 256;
    }
    async start(context) {
        const abort = () => {
            void this.close(new Context().bounded(1000));
        };
        context.signal().addEventListener("abort", abort, { once: true });
        this.#removeOwner = () => {
            context.signal().removeEventListener("abort", abort);
        };
        try {
            if (context.cancelled())
                throw new Error("RabbitMQ startup context cancelled");
            await this.open(context);
        }
        catch (error) {
            await this.close(context);
            throw error;
        }
    }
    open(context) {
        if (this.stopped)
            return Promise.reject(new Error("RabbitMQ stopped"));
        this.epoch++;
        const epoch = this.epoch;
        const worker = new Worker(new URL("./worker.js", import.meta.url), {
            workerData: {
                config: this.config,
                endpoint: this.ep,
                source: this.source,
                server: this.#attempts
            },
            stdout: true,
            stderr: true
        });
        // SDK logs connection IDs; consume streams without retaining buffers.
        worker.stdout.on("data", () => undefined);
        worker.stderr.on("data", () => undefined);
        this.worker = worker;
        let started = false;
        let startupTimer;
        const opening = new Promise((resolve, reject) => {
            startupTimer = setTimeout(() => {
                reject(new Error("RabbitMQ connection startup deadline exceeded"));
            }, this.config.connectTimeoutMs);
            const failure = (error) => {
                if (epoch !== this.epoch || this.stopped)
                    return;
                if (!started)
                    reject(error);
                else
                    this.invalidate(error);
            };
            worker.on("error", () => {
                failure(new Error("RabbitMQ native worker failed"));
            });
            worker.on("exit", () => {
                failure(new Error("RabbitMQ native worker exited"));
            });
            worker.on("message", (event) => {
                if (epoch !== this.epoch || this.stopped)
                    return;
                if (event.kind === "connected") {
                    clearTimeout(startupTimer);
                    startupTimer = setTimeout(() => {
                        reject(new Error("RabbitMQ topology startup deadline exceeded"));
                    }, this.config.operationTimeoutMs);
                }
                else if (event.kind === "ready") {
                    started = true;
                    this.ready = true;
                    resolve();
                }
                else if (event.kind === "error")
                    failure(new Error(event.error));
                else if (event.kind === "message") {
                    if (!this.paused)
                        this.receive(deserialize(Buffer.from(event.bytes)), event.id, epoch);
                }
                else if (event.kind !== "closed") {
                    const pending = this.#pending.get(event.id);
                    if (!pending)
                        return;
                    this.#pending.delete(event.id);
                    clearTimeout(pending.timer);
                    if (event.kind === "settled" && event.error)
                        pending.reject(new Error(event.error));
                    else
                        pending.resolve(event.kind === "delivery" ? event.delivery : undefined);
                }
            });
        });
        return wait(opening, context, this.config.connectTimeoutMs + this.config.operationTimeoutMs).finally(() => {
            clearTimeout(startupTimer);
        });
    }
    invalidate(error) {
        if (this.#recovering || this.stopped)
            return;
        this.ready = false;
        this.epoch++;
        for (const p of this.#pending.values()) {
            clearTimeout(p.timer);
            p.reject(error);
        }
        this.#pending.clear();
        this.report(error);
        const worker = this.worker;
        this.worker = undefined;
        this.#recovering = (async () => {
            await worker?.terminate();
            while (!this.stopped && !this.paused && this.#attempts++ < 60) {
                try {
                    await wait(new Promise((r) => setTimeout(r, 250)), new Context().withExternalCancellation(this.#controller.signal));
                    await this.open(new Context().withExternalCancellation(this.#controller.signal));
                    return;
                }
                catch {
                    this.ready = false;
                    this.epoch++;
                    const failed = this.worker;
                    this.worker = undefined;
                    await failed?.terminate();
                }
            }
            if (!this.stopped && !this.paused)
                this.report(new Error("RabbitMQ reconnect attempts exhausted"));
        })().finally(() => {
            this.#recovering = undefined;
        });
        void this.#recovering.catch(() => undefined);
    }
    command(command) {
        if (!this.ready || !this.worker || this.stopped)
            throw new Error("RabbitMQ connection unavailable");
        if (this.#pending.size >= this.limit)
            throw new Error("RabbitMQ native pending limit exhausted");
        const id = ++this.#serial;
        const promise = new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.invalidate(new Error("RabbitMQ native deadline exceeded; delivery unknown"));
            }, this.config.operationTimeoutMs);
            this.#pending.set(id, { resolve, reject, timer });
        });
        this.worker.postMessage({ ...command, id });
        return promise;
    }
    publish(encoded, context) {
        const op = this.command({ kind: "publish", bytes: encoded });
        return wait(op, context, this.config.operationTimeoutMs).then((v) => v ?? { outcome: "unknown" }, (error) => {
            // Cancel/timeout must dispose native pending state before another admission.
            this.invalidate(new Error("RabbitMQ publication cancelled or timed out; outcome unknown"));
            return {
                outcome: "unknown",
                error: error instanceof Error ? error : new Error("RabbitMQ publication failed")
            };
        });
    }
    async settle(id, epoch, action, context) {
        if (epoch !== this.epoch || !this.ready)
            throw new Error("RabbitMQ delivery belongs to expired connection");
        await wait(this.command({ kind: "settle", delivery: id, action }), context, this.config.operationTimeoutMs);
    }
    pause() {
        this.paused = true;
        this.worker?.postMessage({ kind: "pause" });
    }
    close(context) {
        if (this.#closing)
            return wait(this.#closing, context, this.config.operationTimeoutMs).catch(() => undefined);
        this.#closing = this.finishClose(context);
        return this.#closing;
    }
    async finishClose(context) {
        this.#removeOwner?.();
        this.#removeOwner = undefined;
        this.stopped = true;
        this.ready = false;
        this.epoch++;
        this.#controller.abort();
        for (const p of this.#pending.values()) {
            clearTimeout(p.timer);
            p.reject(new Error("RabbitMQ stopped; publication outcome unknown"));
        }
        this.#pending.clear();
        const worker = this.worker;
        if (worker) {
            let finish;
            const closed = new Promise((resolve) => {
                finish = resolve;
            });
            const message = (event) => {
                if (event.kind === "closed")
                    finish();
            };
            worker.on("message", message);
            worker.once("exit", finish);
            try {
                worker.postMessage({ kind: "close" });
                await wait(closed, context, this.config.operationTimeoutMs);
            }
            catch {
                /* Native close exceeded its budget; termination below is mandatory. */
            }
            finally {
                worker.off("message", message);
                worker.off("exit", finish);
            }
        }
        this.worker = undefined;
        const cleanup = Promise.allSettled([worker?.terminate(), this.#recovering]);
        await wait(cleanup, context, this.config.operationTimeoutMs).catch(() => undefined);
    }
}
export function snapshot(message, context) {
    if (message.to !== undefined)
        throw new Error("RabbitMQ destination cannot override modeled endpoint");
    const body = message.body;
    if (body !== undefined && typeof body !== "string" && !(body instanceof Uint8Array))
        throw new Error("RabbitMQ JS worker profile requires bytes or string body; native codec objects cannot cross owners");
    const copy = deserialize(serialize({ ...message, ...(body instanceof Uint8Array ? { body: Buffer.from(body) } : {}) }));
    const properties = { ...copy.application_properties };
    for (const [key, value] of context.transportMetadata()) {
        for (const existing of Object.keys(properties))
            if (existing.toLowerCase() === key.toLowerCase())
                Reflect.deleteProperty(properties, existing);
        properties[key] = value;
    }
    copy.application_properties = properties;
    const encoded = serialize(copy);
    if (encoded.length > MESSAGE_LIMIT || encoded.length > BYTE_LIMIT)
        throw new Error("RabbitMQ message exceeds 1 MiB admission profile");
    return encoded;
}
//# sourceMappingURL=client.js.map