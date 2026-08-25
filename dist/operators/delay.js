import { ConsumedStream, durableCallDelay, errorFromUnknown, spanError, stringAttribute } from "../runtime/index.js";
export class DelayStream extends ConsumedStream {
    #function;
    constructor(config, source, function_) {
        const environment = source.runtimeEnvironment();
        super(config, environment, source.serde());
        this.#function = function_;
        source.setConsumer(this);
        environment.registerStream(this);
    }
    functionImplementation() {
        return this.#function;
    }
    async consume(context, value) {
        const started = this.startSpan(context, "stream.delay");
        const spanContext = started?.context ?? context;
        let duration;
        try {
            duration = await this.#function.duration(spanContext, this, value);
        }
        catch (error) {
            started?.span.end();
            throw error;
        }
        if (duration <= 0) {
            try {
                await this.emit(spanContext, value);
            }
            finally {
                started?.span.end();
            }
            return;
        }
        try {
            if (await durableCallDelay(spanContext, duration)) {
                try {
                    await this.emit(spanContext, value);
                }
                finally {
                    started?.span.end();
                }
                return;
            }
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(started?.span, failure);
            try {
                await this.#function.delayError(spanContext, this, value, failure, this);
            }
            finally {
                started?.span.end();
            }
            return;
        }
        try {
            this.runtimeEnvironment().delay(spanContext, duration, async () => {
                try {
                    if (spanContext.cancelled()) {
                        started?.span.addEvent("delay.skipped", [
                            stringAttribute("reason", cancellationReason(spanContext))
                        ]);
                        return;
                    }
                    await this.emit(spanContext, value);
                }
                finally {
                    started?.span.end();
                }
            });
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(started?.span, failure);
            try {
                await this.#function.delayError(spanContext, this, value, failure, this);
            }
            finally {
                started?.span.end();
            }
        }
    }
}
function cancellationReason(context) {
    const reason = context.signal().reason;
    return reason instanceof Error ? reason.message : "context cancelled";
}
export function makeDelayStream(config, source, function_) {
    return new DelayStream(config, source, function_);
}
//# sourceMappingURL=delay.js.map