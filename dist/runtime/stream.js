import { transformationName } from "./config/types.js";
import { stringAttribute } from "./environment/tracing/tracing.js";
/**
 * Direct delivery preserves FunctionCall semantics. The async bit is graph
 * metadata and never turns this call into detached work.
 */
export class FunctionCaller {
    #consumer;
    #async;
    constructor(consumer, async = false) {
        this.#consumer = consumer;
        this.#async = async;
    }
    isAsync() {
        return this.#async;
    }
    consume(context, value) {
        return this.#consumer.consume(context, value);
    }
}
/** Stores only immutable graph identity; reloadable config is resolved elsewhere by ID. */
export class ServiceStream {
    #id;
    #environment;
    #name;
    #tracer;
    transformationName;
    constructor(config, environment) {
        this.#id = config.id;
        this.#environment = environment;
        this.#tracer = environment.tracing()?.tracer(environment.serviceConfig().name);
        this.#name = config.name;
        this.transformationName = transformationName(config.type);
    }
    get id() {
        return this.#id;
    }
    get name() {
        return this.#name;
    }
    runtimeEnvironment() {
        return this.#environment;
    }
    config() {
        const config = this.#environment.runtimeConfig().streamById(this.#id);
        if (config === undefined) {
            throw new Error(`stream config ${String(this.#id)} not found`);
        }
        return config;
    }
    tracingEnabled(context) {
        return this.#tracer !== undefined && context.samplingEnabled();
    }
    startSpan(context, operation) {
        if (!this.tracingEnabled(context)) {
            return undefined;
        }
        return this.#tracer?.start(context, operation, [stringAttribute("stream", this.name)]);
    }
    traceCompletion(context, operation, consume) {
        const started = this.startSpan(context, operation);
        if (started === undefined) {
            return consume(context);
        }
        let completion;
        try {
            completion = consume(started.context);
        }
        catch (error) {
            started.span.end();
            throw error;
        }
        if (completion === undefined) {
            started.span.end();
            return;
        }
        return completion.finally(() => {
            started.span.end();
        });
    }
}
//# sourceMappingURL=stream.js.map