import { ConsumedStream } from "../runtime/consumed-stream.js";
import { MessageContextKey } from "../runtime/context.js";
import { errorFromUnknown } from "../runtime/errors.js";
import { StreamLink } from "./stream-link.js";
class SubStreamCall {
    #completion = Promise.withResolvers();
    #signal;
    #abort;
    #context;
    #collector;
    #active;
    #closed = false;
    #failure;
    constructor(context, collector) {
        this.#context = context;
        this.#collector = collector;
        this.#signal = context.signal();
        this.#abort = () => {
            this.finish(errorFromUnknown(this.#signal.reason ?? new Error("substream cancelled")));
        };
        this.#signal.addEventListener("abort", this.#abort, { once: true });
        if (context.cancelled())
            this.#abort();
    }
    completion() {
        return this.#completion.promise;
    }
    throwIfFailed() {
        if (this.#failure !== undefined)
            throw this.#failure;
    }
    deliver(value) {
        if (this.#closed)
            return;
        if (this.#active !== undefined) {
            return this.#active.then(() => this.deliver(value));
        }
        const context = this.#context;
        const collector = this.#collector;
        if (context === undefined || collector === undefined)
            return;
        if (context.cancelled()) {
            this.#abort();
            return;
        }
        try {
            const complete = collector.out(context, value);
            if (typeof complete === "boolean") {
                if (complete)
                    this.finish();
                return;
            }
            this.#active = complete
                .then((done) => {
                if (done)
                    this.finish();
            }, (error) => {
                this.finish(errorFromUnknown(error));
                throw error;
            })
                .finally(() => {
                this.#active = undefined;
            });
            return this.#active;
        }
        catch (error) {
            this.finish(errorFromUnknown(error));
            throw error;
        }
    }
    finish(error) {
        if (this.#closed)
            return;
        this.#closed = true;
        this.#failure = error;
        this.#collector = undefined;
        this.#context = undefined;
        this.#signal.removeEventListener("abort", this.#abort);
        this.#completion.resolve(undefined);
    }
    async close() {
        this.finish();
        // Cancellation stops new callbacks, but does not return while one is active.
        try {
            await this.#active;
        }
        catch {
            // The original body/collector error is reported by consume or its caller.
        }
    }
}
class ResultLink extends StreamLink {
    key;
    constructor(entry, key) {
        super(entry);
        this.key = key;
    }
    consume(context, value) {
        return context.localValue(this.key)?.deliver(value);
    }
}
/** A service-local callable entry; source is the ordinary result-producing stream. */
export class SubStream extends ConsumedStream {
    #call = new MessageContextKey(undefined);
    #source;
    constructor(config, environment, serde) {
        super(config, environment, serde);
        if (config.idService !== environment.serviceConfig().id) {
            throw new Error("SubStream must belong to its execution service");
        }
        environment.registerStream(this);
        environment.registerRuntimeBuildable(this);
    }
    setConsumer(consumer) {
        if (consumer.config().idService !== this.config().idService) {
            throw new Error("SubStream body must belong to the same service");
        }
        super.setConsumer(consumer);
    }
    setSource(source) {
        if (source.id === this.id || source.config().idService !== this.config().idService) {
            throw new Error("SubStream source must be a different stream in the same service");
        }
        if (this.#source !== undefined) {
            if (source === this.#source)
                return;
            throw new Error("SubStream source is already configured");
        }
        source.setConsumer(new ResultLink(this, this.#call));
        this.#source = source;
    }
    build() {
        if (this.consumer() === undefined || this.#source === undefined) {
            throw new Error(`SubStream ${this.name} requires a body and a result source`);
        }
    }
    async consume(context, value, collector) {
        this.build();
        const call = new SubStreamCall(context, collector);
        try {
            call.throwIfFailed();
            const invocation = context.withLocalValue(this.#call, call);
            if (!this.tracingEnabled(invocation)) {
                await this.dispatch(invocation, value, call);
            }
            else {
                await this.traceCompletion(invocation, "stream.substream", (spanContext) => this.dispatch(spanContext, value, call));
            }
        }
        finally {
            await call.close();
        }
    }
    async dispatch(context, value, call) {
        await this.emit(context, value);
        const environment = this.runtimeEnvironment();
        if (environment.waitSubStreamResult === undefined) {
            await call.completion();
        }
        else {
            await environment.waitSubStreamResult(call.completion());
        }
        call.throwIfFailed();
    }
}
export function makeSubStream(config, environment) {
    return new SubStream(config, environment, environment.serdeByName(config.valueType));
}
//# sourceMappingURL=substream.js.map