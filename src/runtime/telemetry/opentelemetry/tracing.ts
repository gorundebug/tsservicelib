import {
  ROOT_CONTEXT,
  SpanStatusCode as OpenTelemetrySpanStatusCode,
  trace,
  type Attributes,
  type Context as OpenTelemetryContext,
  type Span as OpenTelemetrySpan,
  type TextMapGetter,
  type TextMapSetter,
  type Tracer as OpenTelemetryTracer
} from "@opentelemetry/api";
import {
  CompositePropagator,
  W3CBaggagePropagator,
  W3CTraceContextPropagator
} from "@opentelemetry/core";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
  AlwaysOnSampler,
  BatchSpanProcessor,
  NodeTracerProvider,
  ParentBasedSampler,
  type BufferConfig,
  type SpanExporter
} from "@opentelemetry/sdk-trace-node";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
import {
  OpenTelemetryPlugin,
  OpenTelemetryWorkflowClientInterceptor
} from "@temporalio/interceptors-opentelemetry";
import type { WorkflowClientInterceptor } from "@temporalio/client";
import type { WorkerPlugin } from "@temporalio/worker";

import type { Context, MessageContext } from "../../context.js";
import {
  SpanStatusCode,
  type Attribute,
  type Span,
  type SpanContext,
  type StartedSpan,
  type Tracer,
  type Tracing,
  type TracingEngine
} from "../../environment/index.js";

export interface OpenTelemetryTracingOptions {
  readonly serviceName: string;
  readonly endpoint?: string;
  readonly exportTimeoutMillis?: number;
  readonly exporter?: SpanExporter;
  readonly batch?: BufferConfig;
  readonly resourceAttributes?: Readonly<Record<string, string | number | boolean>>;
}

const propagator = new CompositePropagator({
  propagators: [new W3CTraceContextPropagator(), new W3CBaggagePropagator()]
});

const metadataGetter: TextMapGetter<ReadonlyMap<string, string>> = {
  keys(carrier): string[] {
    return [...carrier.keys()];
  },
  get(carrier, key): string | undefined {
    return carrier.get(key.toLowerCase());
  }
};

const metadataSetter: TextMapSetter<Map<string, string>> = {
  set(carrier, key, value): void {
    carrier.set(key.toLowerCase(), value);
  }
};

export class OpenTelemetryTracingEngine implements TracingEngine {
  readonly #provider: NodeTracerProvider;
  readonly #tracing: Tracing;
  #shutdown: Promise<void> | undefined;

  public constructor(options: OpenTelemetryTracingOptions) {
    const exporter = options.exporter ?? makeExporter(options);
    const processor = new BatchSpanProcessor(exporter, options.batch);
    const resource = resourceFromAttributes({
      ...options.resourceAttributes,
      [ATTR_SERVICE_NAME]: options.serviceName
    });
    this.#provider = new NodeTracerProvider({
      resource,
      sampler: new ParentBasedSampler({ root: new AlwaysOnSampler() }),
      spanProcessors: [processor]
    });
    this.#tracing = new OpenTelemetryTracing(
      this.#provider,
      // Temporal 1.21 pins the OTel 1.x structural interfaces while the
      // application runtime uses OTel 2.x. SpanProcessor and Resource keep the
      // same runtime contract; isolate the compatibility boundary here.
      new OpenTelemetryPlugin({
        tracer: this.#provider.getTracer("@gorundebug/tsservicelib-temporal"),
        resource,
        spanProcessor: processor as never
      })
    );
  }

  public tracing(): Tracing {
    return this.#tracing;
  }

  public async shutdown(context: Context): Promise<void> {
    this.#shutdown ??= this.#provider.shutdown();
    await waitForShutdown(this.#shutdown, context.signal());
  }
}

class OpenTelemetryTracing implements Tracing {
  readonly #provider: NodeTracerProvider;
  readonly #temporalPlugin: OpenTelemetryPlugin;

  public constructor(provider: NodeTracerProvider, temporalPlugin: OpenTelemetryPlugin) {
    this.#provider = provider;
    this.#temporalPlugin = temporalPlugin;
  }

  public enabled(): boolean {
    return true;
  }

  public tracer(name: string): Tracer {
    return new TracerAdapter(this.#provider.getTracer(name));
  }

  public temporalWorkerPlugin(): WorkerPlugin {
    return this.#temporalPlugin;
  }

  public temporalWorkflowClientInterceptor(): WorkflowClientInterceptor {
    return new OpenTelemetryWorkflowClientInterceptor({
      tracer: this.#provider.getTracer("@gorundebug/tsservicelib-temporal-client")
    });
  }
}

class TracerAdapter implements Tracer {
  readonly #tracer: OpenTelemetryTracer;

  public constructor(tracer: OpenTelemetryTracer) {
    this.#tracer = tracer;
  }

  public start(
    context: MessageContext,
    spanName: string,
    attributes: readonly Attribute[] = []
  ): StartedSpan {
    const parent = parentContext(context);
    const span = this.#tracer.startSpan(
      spanName,
      { attributes: attributesToOpenTelemetry(attributes) },
      parent
    );
    const active = trace.setSpan(parent, span);
    const metadata = new Map(context.metadata());
    propagator.inject(active, metadata, metadataSetter);
    return {
      context: context.withMetadata(metadata).withOpenTelemetryContext(active),
      span: new SpanAdapter(span)
    };
  }
}

class SpanAdapter implements Span {
  readonly #span: OpenTelemetrySpan;
  #ended = false;

  public constructor(span: OpenTelemetrySpan) {
    this.#span = span;
  }

  public end(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#span.end();
  }

  public setAttributes(attributes: readonly Attribute[]): void {
    this.#span.setAttributes(attributesToOpenTelemetry(attributes));
  }

  public recordError(error: Error): void {
    this.#span.recordException(error);
  }

  public setStatus(code: SpanStatusCode, description: string): void {
    const statusCode = statusCodeToOpenTelemetry(code);
    if (statusCode === OpenTelemetrySpanStatusCode.ERROR) {
      this.#span.setStatus({ code: statusCode, message: description });
      return;
    }
    this.#span.setStatus({ code: statusCode });
  }

  public addEvent(name: string, attributes: readonly Attribute[] = []): void {
    this.#span.addEvent(name, attributesToOpenTelemetry(attributes));
  }

  public spanContext(): SpanContext {
    const context = this.#span.spanContext();
    return {
      traceId: context.traceId,
      spanId: context.spanId,
      isValid: trace.isSpanContextValid(context)
    };
  }
}

function makeExporter(options: OpenTelemetryTracingOptions): SpanExporter {
  const exporterOptions: { url?: string; timeoutMillis?: number } = {};
  if (options.endpoint !== undefined) {
    exporterOptions.url = options.endpoint;
  }
  if (options.exportTimeoutMillis !== undefined) {
    exporterOptions.timeoutMillis = options.exportTimeoutMillis;
  }
  return new OTLPTraceExporter(exporterOptions);
}

function parentContext(context: MessageContext): OpenTelemetryContext {
  const current = context.openTelemetryContext();
  if (current !== undefined) {
    return current;
  }
  return propagator.extract(ROOT_CONTEXT, context.metadata(), metadataGetter);
}

function attributesToOpenTelemetry(attributes: readonly Attribute[]): Attributes {
  const result: Attributes = {};
  for (const attribute of attributes) {
    result[attribute.key] = attributeValue(attribute);
  }
  return result;
}

function attributeValue(attribute: Attribute): string | number | boolean {
  if (attribute.type !== "int64") {
    return attribute.value;
  }
  const value = Number(attribute.value);
  if (!Number.isSafeInteger(value)) {
    throw new RangeError(
      `OpenTelemetry JS cannot represent int64 attribute ${attribute.key}=${attribute.value.toString()} exactly`
    );
  }
  return value;
}

function statusCodeToOpenTelemetry(code: SpanStatusCode): OpenTelemetrySpanStatusCode {
  switch (code) {
    case SpanStatusCode.Ok:
      return OpenTelemetrySpanStatusCode.OK;
    case SpanStatusCode.Error:
      return OpenTelemetrySpanStatusCode.ERROR;
    case SpanStatusCode.Unset:
      return OpenTelemetrySpanStatusCode.UNSET;
  }
}

async function waitForShutdown(shutdown: Promise<void>, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    void shutdown.catch(() => undefined);
    throw cancellationError(signal);
  }
  let rejectCancellation: ((reason: Error) => void) | undefined;
  const cancelled = new Promise<never>((_resolve, reject) => {
    rejectCancellation = reject;
  });
  const onAbort = (): void => {
    rejectCancellation?.(cancellationError(signal));
  };
  signal.addEventListener("abort", onAbort, { once: true });
  try {
    await Promise.race([shutdown, cancelled]);
  } finally {
    signal.removeEventListener("abort", onAbort);
    void shutdown.catch(() => undefined);
  }
}

function cancellationError(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error("tracing shutdown cancelled");
}
