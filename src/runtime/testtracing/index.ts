import type { Context, MessageContext } from "../context.js";
import type {
  Attribute,
  Span,
  SpanContext,
  SpanStatusCode,
  StartedSpan,
  Tracer,
  Tracing,
  TracingEngine
} from "../environment/index.js";
import { SpanStatusCode as Status } from "../environment/index.js";

export interface RecordedEvent {
  readonly name: string;
  readonly attributes: readonly Attribute[];
}

export interface RecordedSpan {
  readonly tracerName: string;
  readonly name: string;
  readonly attributes: readonly Attribute[];
  readonly events: readonly RecordedEvent[];
  readonly statusCode: SpanStatusCode;
  readonly statusDescription: string;
  readonly error: Error | undefined;
}

export class TestTracing implements Tracing, TracingEngine {
  readonly #spans: RecordedSpan[] = [];

  public enabled(): boolean {
    return true;
  }

  public tracing(): Tracing {
    return this;
  }

  public tracer(name: string): Tracer {
    return new TestTracer(this, name);
  }

  public spans(): readonly RecordedSpan[] {
    return this.#spans.map((span) => ({
      ...span,
      attributes: [...span.attributes],
      events: span.events.map((event) => ({ ...event, attributes: [...event.attributes] }))
    }));
  }

  public reset(): void {
    this.#spans.length = 0;
  }

  public shutdown(context: Context): Promise<void> {
    void context;
    return Promise.resolve();
  }

  public record(span: RecordedSpan): void {
    this.#spans.push(span);
  }
}

class TestTracer implements Tracer {
  readonly #engine: TestTracing;
  readonly #name: string;

  public constructor(engine: TestTracing, name: string) {
    this.#engine = engine;
    this.#name = name;
  }

  public start(
    context: MessageContext,
    spanName: string,
    attributes: readonly Attribute[] = []
  ): StartedSpan {
    return {
      context,
      span: new TestSpan(this.#engine, this.#name, spanName, attributes)
    };
  }
}

class TestSpan implements Span {
  readonly #engine: TestTracing;
  readonly #tracerName: string;
  readonly #name: string;
  readonly #attributes: Attribute[];
  readonly #events: RecordedEvent[] = [];
  #statusCode: SpanStatusCode = Status.Unset;
  #statusDescription = "";
  #error: Error | undefined;
  #ended = false;

  public constructor(
    engine: TestTracing,
    tracerName: string,
    name: string,
    attributes: readonly Attribute[]
  ) {
    this.#engine = engine;
    this.#tracerName = tracerName;
    this.#name = name;
    this.#attributes = [...attributes];
  }

  public end(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#engine.record({
      tracerName: this.#tracerName,
      name: this.#name,
      attributes: [...this.#attributes],
      events: [...this.#events],
      statusCode: this.#statusCode,
      statusDescription: this.#statusDescription,
      error: this.#error
    });
  }

  public setAttributes(attributes: readonly Attribute[]): void {
    this.#attributes.push(...attributes);
  }

  public recordError(error: Error): void {
    this.#error = error;
  }

  public setStatus(code: SpanStatusCode, description: string): void {
    this.#statusCode = code;
    this.#statusDescription = description;
  }

  public addEvent(name: string, attributes: readonly Attribute[] = []): void {
    this.#events.push({ name, attributes: [...attributes] });
  }

  public spanContext(): SpanContext {
    return { traceId: "", spanId: "", isValid: false };
  }
}
