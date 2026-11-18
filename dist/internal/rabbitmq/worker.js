import { parentPort, workerData } from "node:worker_threads";
import { readFile } from "node:fs/promises";
import { serialize, deserialize } from "node:v8";
import { createEnvironment, OutcomeState } from "rabbitmq-amqp-js-client";
import { BYTE_LIMIT, MESSAGE_LIMIT } from "./protocol.js";
const port = parentPort;
if (!port)
    throw new Error("RabbitMQ worker requires an owner");
const setup = workerData;
const { config: c, endpoint: ep } = setup;
const limit = (setup.source ? ep.consumerCredits : ep.publisherMaxInFlight) ?? 256;
let connection;
let consumer;
let publisher;
let paused = false, failed = false, serial = 0, bytes = 0, unadmitted = 0;
const unsettled = new Map();
const post = (event) => {
    port.postMessage(event);
};
function fail(error) {
    if (failed)
        return;
    failed = true;
    paused = true;
    post({ kind: "error", error });
    consumer?.close();
    void connection?.close().catch(() => undefined);
}
async function start() {
    const url = new URL(c.servers[setup.server % c.servers.length] ?? "");
    const secure = c.tlsEnabled === true || url.protocol === "amqps:";
    const environment = createEnvironment({
        host: url.hostname.replace(/^\[|\]$/gu, ""),
        port: Number(url.port || (secure ? 5671 : 5672)),
        username: c.username ?? "",
        password: c.password ?? "",
        ...(secure
            ? {
                tls: {
                    rejectUnauthorized: true,
                    ...(c.tlsCaFile ? { ca: await readFile(c.tlsCaFile, "utf8") } : {}),
                    ...(c.tlsCertFile
                        ? {
                            cert: await readFile(c.tlsCertFile, "utf8"),
                            key: await readFile(c.tlsKeyFile ?? "", "utf8")
                        }
                        : {})
                }
            }
            : {})
    });
    connection = await environment.createConnection({ reconnect: false });
    post({ kind: "connected" });
    const management = connection.management();
    let queue;
    if (ep.queueName) {
        try {
            queue = await management.getQueueInfo(ep.queueName, c.operationTimeoutMs);
        }
        catch (error) {
            if (!ep.createQueue || !String(error).includes("404"))
                throw new Error("RabbitMQ queue missing or inaccessible", {
                    cause: error
                });
            queue = await management.declareQueue(ep.queueName, { type: ep.queueType ?? "quorum", durable: true }, c.operationTimeoutMs);
        }
        if (ep.queueType && queue.getInfo.type !== ep.queueType)
            throw new Error("RabbitMQ existing queue type mismatch");
    }
    if (ep.createExchange && ep.exchangeName)
        await management.declareExchange(ep.exchangeName, { type: ep.exchangeType ?? "direct", durable: true }, c.operationTimeoutMs);
    if (ep.createBinding && ep.exchangeName && queue)
        await management.bind(ep.routingKey ?? "", {
            source: {
                getInfo: {
                    name: ep.exchangeName,
                    type: ep.exchangeType ?? "direct",
                    arguments: {},
                    autoDelete: false,
                    durable: true
                }
            },
            destination: queue
        }, c.operationTimeoutMs);
    // Sender attach verifies existing exchange without provisioning it.
    if (!setup.source || ep.exchangeName)
        publisher = await connection.createPublisher(ep.exchangeName
            ? {
                exchange: {
                    name: ep.exchangeName,
                    ...(ep.routingKey ? { routingKey: ep.routingKey } : {})
                }
            }
            : { queue: { name: ep.queueName ?? "" } });
    if (setup.source) {
        consumer = await connection.createConsumer({
            queue: { name: ep.queueName ?? "" },
            preSettled: false,
            messageHandler: (handle, message) => {
                if (failed)
                    return;
                const encoded = serialize(message);
                if (paused) {
                    bytes += encoded.length;
                    unadmitted++;
                    if (unsettled.size + unadmitted > limit ||
                        bytes > BYTE_LIMIT ||
                        encoded.length > MESSAGE_LIMIT)
                        fail("RabbitMQ source overflow while draining; unadmitted input remains unsettled");
                    return;
                }
                if (unsettled.size >= limit ||
                    encoded.length > MESSAGE_LIMIT ||
                    bytes + encoded.length > BYTE_LIMIT) {
                    fail("RabbitMQ source admission overflow; unsettled input will be redelivered");
                    return;
                }
                const id = ++serial;
                unsettled.set(id, { handle, size: encoded.length });
                bytes += encoded.length;
                post({ kind: "message", id, bytes: encoded });
            }
        });
        consumer.start();
    }
    setInterval(() => {
        if (!paused && !connection?.isOpen())
            fail("RabbitMQ connection disconnected");
    }, 25);
    post({ kind: "ready" });
}
port.on("message", (cmd) => {
    if (cmd.kind === "pause") {
        paused = true;
        return;
    }
    if (cmd.kind === "close") {
        paused = true;
        void (async () => {
            try {
                await connection?.close();
            }
            catch {
                /* Owner will terminate transport. */
            }
            finally {
                post({ kind: "closed" });
            }
        })();
        return;
    }
    if (cmd.kind === "settle") {
        const item = unsettled.get(cmd.delivery);
        try {
            if (!item || failed)
                throw new Error();
            if (cmd.action === "accept")
                item.handle.accept();
            else if (cmd.action === "release")
                item.handle.requeue();
            else
                item.handle.discard();
            unsettled.delete(cmd.delivery);
            bytes -= item.size;
            post({ kind: "settled", id: cmd.id });
        }
        catch {
            post({
                kind: "settled",
                id: cmd.id,
                error: "RabbitMQ settlement handle expired or native settlement failed"
            });
        }
        return;
    }
    if (!publisher || failed) {
        post({ kind: "delivery", id: cmd.id, delivery: { outcome: "unknown" } });
        return;
    }
    const message = deserialize(Buffer.from(cmd.bytes));
    void publisher.publish(message).then((result) => {
        // AMQP composite state contains codec functions; only its public error
        // properties cross owners, never the live Delivery or codec internals.
        const state = result.delivery.remote_state;
        post({
            kind: "delivery",
            id: cmd.id,
            delivery: {
                outcome: result.outcome === OutcomeState.ACCEPTED
                    ? "accepted"
                    : result.outcome === OutcomeState.RELEASED
                        ? "unroutable"
                        : "rejected",
                nativeOutcome: result.outcome,
                remoteState: state?.error
                    ? { condition: state.error.condition, description: state.error.description }
                    : undefined
            }
        });
    }, () => {
        post({ kind: "delivery", id: cmd.id, delivery: { outcome: "unknown" } });
        fail("RabbitMQ native publication failed");
    });
});
void start().catch(() => {
    fail("RabbitMQ startup failed: connection, permissions or topology incompatible");
});
//# sourceMappingURL=worker.js.map