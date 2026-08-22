import type { Context, MessageContext } from "../../context.js";
import type {
  Attribute,
  Span,
  SpanContext,
  SpanStatusCode,
  StartedSpan,
  Tracer,
  Tracing,
  TracingEngine
} from "./tracing.js";

class NoopSpan implements Span {
  public end(): void {
    return;
  }

  public setAttributes(attributes: readonly Attribute[]): void {
    void attributes;
  }

  public recordError(error: Error): void {
    void error;
  }

  public setStatus(code: SpanStatusCode, description: string): void {
    void code;
    void description;
  }

  public addEvent(name: string, attributes?: readonly Attribute[]): void {
    void name;
    void attributes;
  }

  public spanContext(): SpanContext {
    return invalidSpanContext;
  }
}

class NoopTracer implements Tracer {
  public start(context: MessageContext, spanName: string): StartedSpan {
    void spanName;
    return { context, span: noopSpan };
  }
}

class NoopTracing implements Tracing {
  public enabled(): boolean {
    return false;
  }

  public tracer(name: string): Tracer {
    void name;
    return noopTracer;
  }
}

export class NoopTracingEngine implements TracingEngine {
  public tracing(): Tracing {
    return noopTracing;
  }

  public shutdown(context: Context): Promise<void> {
    void context;
    return Promise.resolve();
  }
}

const invalidSpanContext: SpanContext = { traceId: "", spanId: "", isValid: false };
export const noopSpan: Span = new NoopSpan();
export const noopTracer: Tracer = new NoopTracer();
export const noopTracing: Tracing = new NoopTracing();
export const noopTracingEngine: TracingEngine = new NoopTracingEngine();
