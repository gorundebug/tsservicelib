import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { createClient, RESP_TYPES, ErrorReply, ClientClosedError, ClientOfflineError } from "redis";
import { Context } from "../../runtime/index.js";
import { wait, cancellation } from "./wait.js";
export { wait, cancellation } from "./wait.js";
export const PENDING_MESSAGES = 256;
export const BYTE_LIMIT = 8 * 1024 * 1024;
export const MESSAGE_LIMIT = 1024 * 1024;
export class DeletedPendingEntriesError extends Error {
    ids;
    constructor(ids) {
        super(`RedisStreams XAUTOCLAIM removed ${String(ids.length)} deleted entries from the PEL`);
        this.ids = ids;
    }
}
function nativeClient(options) {
    return createClient({
        ...options,
        RESP: 2,
        commandOptions: { typeMapping: { [RESP_TYPES.BLOB_STRING]: Buffer } }
    });
}
function destroy(client) {
    if (client?.isOpen)
        client.destroy();
}
function array(value) {
    if (!Array.isArray(value))
        throw new Error("RedisStreams unexpected native array reply");
    return value;
}
function text(value) {
    if (Buffer.isBuffer(value))
        return value.toString();
    if (typeof value === "string")
        return value;
    throw new Error("RedisStreams unexpected native string reply");
}
export function entryBytes(fields) {
    return fields.reduce((n, [k, v]) => n + Buffer.byteLength(k) + Buffer.byteLength(v) + 32, 0);
}
function entries(value, reclaimed) {
    return array(value).map((raw) => {
        const [nativeId, values] = array(raw), id = text(nativeId), flat = array(values);
        if (flat.length % 2)
            throw new Error("RedisStreams malformed field/value reply");
        const fields = [];
        let bytes = 0;
        for (let i = 0; i < flat.length; i += 2) {
            const k = flat[i], v = flat[i + 1];
            if (!Buffer.isBuffer(k) || !Buffer.isBuffer(v))
                throw new Error("RedisStreams requires binary native fields");
            bytes += k.length + v.length + 32;
            fields.push([k, v]);
        }
        // Oversized records carry only diagnostic identity, never a synthetic payload
        // into the graph. Copy admitted fields so tiny slices do not retain a large reply.
        return {
            id,
            fields: bytes > MESSAGE_LIMIT
                ? []
                : fields.map(([k, v]) => [Buffer.from(k), Buffer.from(v)]),
            reclaimed,
            bytes
        };
    });
}
export function snapshot(fields, context) {
    if (!fields.length || fields.length > 4096)
        throw new Error("RedisStreams publication requires 1..4096 field/value pairs");
    const copy = fields.map(([k, v]) => [Buffer.from(k), Buffer.from(v)]);
    for (const [key, value] of context.transportMetadata()) {
        for (let i = copy.length - 1; i >= 0; i--)
            if (copy[i]?.[0].toString().toLowerCase() === key.toLowerCase())
                copy.splice(i, 1);
        copy.push([Buffer.from(key), Buffer.from(value)]);
    }
    const bytes = entryBytes(copy);
    if (bytes > MESSAGE_LIMIT)
        throw new Error("RedisStreams entry exceeds 1 MiB admission limit");
    return { fields: copy, bytes };
}
export function entryMetadata(entry) {
    const result = new Map();
    for (const [key, value] of entry.fields) {
        const k = key.toString().toLowerCase();
        if (["x-stream-id", "x-trace", "traceparent", "tracestate", "baggage"].includes(k))
            result.set(k, value.toString());
    }
    return result;
}
export class Session {
    config;
    ep;
    source;
    ready = false;
    epoch = 0;
    stopped = false;
    paused = false;
    limit;
    consumerName;
    #control;
    #reader;
    #generationError;
    #controller = new AbortController();
    #recovering;
    #attempts = 0;
    #removeOwner;
    constructor(config, ep, source, report) {
        this.config = config;
        this.ep = ep;
        this.source = source;
        this.limit = source ? (ep.readCount ?? 64) : (ep.publisherMaxInFlight ?? 256);
        this.consumerName = ep.consumerName ?? `ts-${String(ep.id)}-${randomUUID()}`;
        this.#generationError = report;
    }
    isReady() {
        return this.ready;
    }
    isStopped() {
        return this.stopped;
    }
    async start(context) {
        const abort = () => {
            this.close();
        };
        context.signal().addEventListener("abort", abort, { once: true });
        this.#removeOwner = () => {
            context.signal().removeEventListener("abort", abort);
        };
        try {
            if (context.cancelled())
                throw cancellation(context);
            await this.open(context);
        }
        catch (error) {
            this.close();
            throw error;
        }
    }
    async open(context) {
        if (this.isStopped())
            throw new Error("RedisStreams stopped");
        const c = this.config, u = new URL(c.servers[0] ?? "");
        const secure = c.tlsEnabled === true || u.protocol === "rediss:";
        const [ca, cert, key] = await wait(Promise.all([
            c.tlsCaFile ? readFile(c.tlsCaFile) : undefined,
            c.tlsCertFile ? readFile(c.tlsCertFile) : undefined,
            c.tlsKeyFile ? readFile(c.tlsKeyFile) : undefined
        ]), context, c.connectTimeoutMs);
        if (context.cancelled() || this.isStopped())
            throw new Error("RedisStreams startup cancelled");
        const options = {
            socket: {
                host: u.hostname.replace(/^\[|\]$/gu, ""),
                port: Number(u.port || "6379"),
                connectTimeout: c.connectTimeoutMs,
                reconnectStrategy: false,
                ...(secure
                    ? {
                        tls: true,
                        rejectUnauthorized: true,
                        servername: c.tlsServerName === "" ? u.hostname : (c.tlsServerName ?? u.hostname),
                        ...(ca ? { ca } : {}),
                        ...(cert ? { cert, key } : {})
                    }
                    : { tls: false })
            },
            database: c.redisDatabase,
            disableOfflineQueue: true,
            commandsQueueMaxLength: this.limit + 4,
            ...(c.username ? { username: c.username } : {}),
            ...(c.password ? { password: c.password } : {})
        };
        const control = nativeClient(options), reader = this.source ? nativeClient(options) : undefined;
        this.#control = control;
        this.#reader = reader;
        const epoch = ++this.epoch;
        for (const client of [control, reader])
            client?.on("error", () => {
                if (this.ready && epoch === this.epoch && !(client === reader && this.paused))
                    this.invalidate(new Error("RedisStreams native connection failed"));
            });
        try {
            await wait(Promise.all([control.connect(), reader?.connect()]), context, c.connectTimeoutMs);
            const info = text(await this.direct(control, ["INFO", "server"], context));
            const version = /redis_version:(\d+)\.(\d+)/u.exec(info);
            if (!version ||
                Number(version[1]) < 7 ||
                (Number(version[1]) === 7 && Number(version[2]) < 2))
                throw new Error("RedisStreams requires Redis 7.2 or newer");
            if (!/redis_mode:standalone\r?\n/u.test(info))
                throw new Error("RedisStreams only supports standalone; Sentinel/Cluster are unsupported");
            const role = array(await this.direct(control, ["ROLE"], context));
            if (text(role[0]) !== "master")
                throw new Error("RedisStreams requires a standalone primary, not a replica");
            if (this.source) {
                if (this.ep.createConsumerGroup) {
                    try {
                        await this.direct(control, [
                            "XGROUP",
                            "CREATE",
                            this.ep.streamKey,
                            this.ep.consumerGroup ?? "",
                            this.ep.groupStartId ?? "0-0",
                            "MKSTREAM"
                        ], context);
                    }
                    catch (error) {
                        if (!(error instanceof ErrorReply && error.message.startsWith("BUSYGROUP ")))
                            throw error;
                    }
                }
                const groups = array(await this.direct(control, ["XINFO", "GROUPS", this.ep.streamKey], context));
                if (!groups.some((g) => {
                    const parts = array(g);
                    for (let i = 0; i < parts.length; i += 2)
                        if (text(parts[i]) === "name" && text(parts[i + 1]) === this.ep.consumerGroup)
                            return true;
                    return false;
                }))
                    throw new Error("RedisStreams consumer group is absent; creation is opt-in");
            }
            else {
                const kind = text(await this.direct(control, ["TYPE", this.ep.streamKey], context));
                if (!["none", "stream"].includes(kind))
                    throw new Error("RedisStreams streamKey exists with an incompatible Redis type");
            }
            if (context.cancelled() || this.isStopped())
                throw new Error("RedisStreams startup cancelled");
            this.ready = true;
        }
        catch (error) {
            destroy(control);
            destroy(reader);
            this.#control = undefined;
            this.#reader = undefined;
            if (error instanceof Error && error.message.startsWith("RedisStreams"))
                throw error;
            // Native authentication errors are intentionally sanitized before leaving the transport.
            // eslint-disable-next-line preserve-caught-error
            throw new Error("RedisStreams startup failed: verify authentication, permissions (INFO/ROLE/stream commands), database and resources");
        }
    }
    direct(client, args, context, timeout = this.config.operationTimeoutMs) {
        return wait(client.sendCommand(args), context, timeout);
    }
    invalidate(error) {
        if (this.stopped || this.#recovering)
            return;
        this.ready = false;
        this.epoch++;
        destroy(this.#control);
        destroy(this.#reader);
        this.#control = undefined;
        this.#reader = undefined;
        this.#generationError(error);
        this.#recovering = (async () => {
            while (!this.isStopped() && !this.paused && this.#attempts++ < 60) {
                try {
                    const context = new Context().withExternalCancellation(this.#controller.signal);
                    await wait(new Promise((resolve) => setTimeout(resolve, 250)), context);
                    await this.open(context);
                    return;
                }
                catch {
                    if (!this.isStopped())
                        this.#generationError(new Error("RedisStreams reconnect failed"));
                }
            }
            if (!this.isStopped() && !this.paused) {
                this.#generationError(new Error("RedisStreams reconnect attempts exhausted"));
                this.close();
            }
        })().finally(() => {
            this.#recovering = undefined;
        });
        void this.#recovering.catch(() => undefined);
    }
    async command(args, context, read = false, timeout = this.config.operationTimeoutMs) {
        const client = read ? this.#reader : this.#control;
        if (!this.ready || !client?.isReady || this.isStopped())
            throw new ClientOfflineError();
        if (context.cancelled())
            throw cancellation(context);
        try {
            return await this.direct(client, args, context, timeout);
        }
        catch (error) {
            if (!(error instanceof ErrorReply) && !(read && this.paused))
                this.invalidate(new Error("RedisStreams command interrupted; outcome may be unknown"));
            throw error;
        }
    }
    async publish(fields, context) {
        if (context.cancelled() || !this.ready)
            return {
                outcome: "not-sent",
                error: new Error("RedisStreams cancelled or unavailable before XADD")
            };
        try {
            const reply = await this.command([
                "XADD",
                this.ep.streamKey,
                ...(this.ep.createStream ? [] : ["NOMKSTREAM"]),
                "*",
                ...fields.flatMap(([k, v]) => [Buffer.from(k), Buffer.from(v)])
            ], context);
            if (reply === null)
                return {
                    outcome: "rejected",
                    error: new Error("RedisStreams XADD NOMKSTREAM: stream is absent")
                };
            return { outcome: "published", id: text(reply) };
        }
        catch (error) {
            return {
                outcome: error instanceof ErrorReply
                    ? "rejected"
                    : error instanceof ClientClosedError || error instanceof ClientOfflineError
                        ? "not-sent"
                        : "unknown",
                error: error instanceof ErrorReply
                    ? error
                    : new Error("RedisStreams publication interrupted; outcome may be unknown")
            };
        }
    }
    async ack(id, epoch, context) {
        if (epoch !== this.epoch || !this.ready)
            throw new Error("RedisStreams ACK handle belongs to an expired connection");
        const value = await this.command(["XACK", this.ep.streamKey, this.ep.consumerGroup ?? "", id], context);
        if (typeof value !== "number")
            throw new Error("RedisStreams unexpected XACK reply");
        return value;
    }
    async read(count, context, block = this.ep.readBlockMs ?? 1000) {
        const response = await this.command([
            "XREADGROUP",
            "GROUP",
            this.ep.consumerGroup ?? "",
            this.consumerName,
            "COUNT",
            String(count),
            "BLOCK",
            String(block),
            "STREAMS",
            this.ep.streamKey,
            ">"
        ], context, true, block + this.config.operationTimeoutMs);
        if (response === null)
            return [];
        return array(response).flatMap((stream) => entries(array(stream)[1], false));
    }
    async claim(cursor, count, context) {
        const response = array(await this.command([
            "XAUTOCLAIM",
            this.ep.streamKey,
            this.ep.consumerGroup ?? "",
            this.consumerName,
            String(this.ep.claimIdleMs ?? 60000),
            cursor,
            "COUNT",
            String(count)
        ], context));
        return {
            cursor: text(response[0]),
            entries: entries(response[1], true),
            deleted: array(response[2]).map(text)
        };
    }
    pause() {
        this.paused = true;
        destroy(this.#reader);
        this.#reader = undefined;
    }
    close() {
        if (this.isStopped())
            return;
        this.#removeOwner?.();
        this.#removeOwner = undefined;
        this.stopped = true;
        this.ready = false;
        this.epoch++;
        this.#controller.abort();
        destroy(this.#reader);
        destroy(this.#control);
        this.#reader = undefined;
        this.#control = undefined;
    }
}
//# sourceMappingURL=client.js.map