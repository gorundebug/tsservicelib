import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { DelayStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import { durableCallDelay } from "../runtime/durable-call-context.js";
import { errorFromUnknown } from "../runtime/errors.js";
import { spanError, stringAttribute } from "../runtime/environment/tracing/tracing.js";
import type { TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { DelayFunction } from "./functions.js";

export class DelayStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
  readonly #function: DelayFunction<T>;

  public constructor(
    config: DelayStreamConfig,
    source: TypedStream<T>,
    function_: DelayFunction<T>
  ) {
    const environment = source.runtimeEnvironment();
    super(config, environment, source.serde());
    this.#function = function_;
    source.setConsumer(this);
    environment.registerStream(this);
  }

  public functionImplementation(): DelayFunction<T> {
    return this.#function;
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    const started = this.startSpan(context, "stream.delay");
    const spanContext = started?.context ?? context;
    let duration: number;
    try {
      duration = await this.#function.duration(spanContext, this, value);
    } catch (error: unknown) {
      started?.span.end();
      throw error;
    }
    if (duration <= 0) {
      try {
        await this.emit(spanContext, value);
      } finally {
        started?.span.end();
      }
      return;
    }
    try {
      if (await durableCallDelay(spanContext, duration)) {
        try {
          await this.emit(spanContext, value);
        } finally {
          started?.span.end();
        }
        return;
      }
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(started?.span, failure);
      try {
        await this.#function.delayError(spanContext, this, value, failure, this);
      } finally {
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
        } finally {
          started?.span.end();
        }
      });
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(started?.span, failure);
      try {
        await this.#function.delayError(spanContext, this, value, failure, this);
      } finally {
        started?.span.end();
      }
    }
  }
}

function cancellationReason(context: MessageContext): string {
  const reason = context.signal().reason as unknown;
  return reason instanceof Error ? reason.message : "context cancelled";
}

export function makeDelayStream<T>(
  config: DelayStreamConfig,
  source: TypedStream<T>,
  function_: DelayFunction<T>
): DelayStream<T> {
  return new DelayStream(config, source, function_);
}
