import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import ts from "typescript";
import {
  ConsumedStream,
  MessageContext,
  ServiceStream,
  int32SerdeType,
  type Attribute,
  type Completion,
  type DelayStreamConfig,
  type StartedSpan,
  type StreamConfig,
  type Tracer,
  type Tracing
} from "@gorundebug/tsservicelib/runtime";
import { DelayStream } from "@gorundebug/tsservicelib/operators";
import { makeTestEnvironment } from "./support/environment.js";

const config: StreamConfig = {
  id: 1,
  name: "Price",
  type: "Input",
  pipeline: "pricing",
  component: "Customer Pricing",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0,
  properties: {}
};

function noop(): void {
  // This probe records span creation and completion, not span mutations.
}

function parentOf(node: ts.Node): ts.Node | undefined {
  return ts.isSourceFile(node) ? undefined : node.parent;
}

class RecordingTracer implements Tracer {
  public readonly attributes: (readonly Attribute[] | undefined)[] = [];
  public ended = 0;

  public start(
    context: MessageContext,
    _name: string,
    attributes?: readonly Attribute[]
  ): StartedSpan {
    this.attributes.push(attributes);
    return {
      context,
      span: {
        end: () => {
          this.ended += 1;
        },
        setAttributes: noop,
        recordError: noop,
        setStatus: noop,
        addEvent: noop,
        spanContext: () => ({ traceId: "", spanId: "", isValid: false })
      }
    };
  }
}

function tracing(tracer: Tracer): Tracing {
  return { enabled: () => true, tracer: () => tracer };
}

class SamplingContext extends MessageContext {
  public sampled = false;
  public samplingReads = 0;

  public override samplingEnabled(): boolean {
    this.samplingReads += 1;
    return this.sampled;
  }
}

class ObservedStream extends ServiceStream {
  public spanCalls = 0;

  protected override startSpan(
    context: MessageContext,
    operation: string
  ): StartedSpan | undefined {
    this.spanCalls += 1;
    return super.startSpan(context, operation);
  }

  public deliver(
    context: MessageContext,
    consume: (context: MessageContext) => Completion
  ): Completion {
    return this.traceCompletion(context, "stream.test", consume);
  }
}

class ObservedDelay extends DelayStream<number> {
  public spanCalls = 0;

  protected override startSpan(
    context: MessageContext,
    operation: string
  ): StartedSpan | undefined {
    this.spanCalls += 1;
    return super.startSpan(context, operation);
  }
}

for (const enabled of [false, true]) {
  for (const sampled of [false, true]) {
    await test(`completion caller guard: tracer=${String(enabled)}, sampled=${String(sampled)}`, async () => {
      const tracer = new RecordingTracer();
      const environment = makeTestEnvironment(
        [config],
        enabled ? { tracing: tracing(tracer) } : {}
      );
      const stream = new ObservedStream(config, environment);
      const context = new SamplingContext();
      context.sampled = sampled;
      let consumed = 0;
      await stream.deliver(context, (received) => {
        assert.equal(received, context);
        consumed += 1;
      });
      assert.equal(consumed, 1);
      assert.equal(stream.spanCalls, enabled && sampled ? 1 : 0);
      assert.equal(tracer.ended, enabled && sampled ? 1 : 0);
      if (!enabled) assert.equal(context.samplingReads, 0);
    });

    await test(`Delay caller guard: tracer=${String(enabled)}, sampled=${String(sampled)}`, async () => {
      const delayConfig: DelayStreamConfig = {
        ...config,
        id: 2,
        name: "Delay Price",
        idSource: 1,
        type: "Delay",
        duration: 0
      };
      const tracer = new RecordingTracer();
      const environment = makeTestEnvironment(
        [config, delayConfig],
        enabled ? { tracing: tracing(tracer) } : {}
      );
      const source = new ConsumedStream(config, environment, environment.serde(int32SerdeType));
      let consumed = 0;
      const delay = new ObservedDelay(delayConfig, source, {
        duration: (_context, _stream, value) => {
          consumed += value;
          return 0;
        },
        delayError: () => {
          throw new Error("Unexpected delay error");
        }
      });
      const context = new SamplingContext();
      context.sampled = sampled;
      await delay.consume(context, 3);
      assert.equal(consumed, 3);
      assert.equal(delay.spanCalls, enabled && sampled ? 1 : 0);
      assert.equal(tracer.ended, enabled && sampled ? 1 : 0);
      if (!enabled) assert.equal(context.samplingReads, 0);
    });
  }
}

await test("sampled stream spans reuse frozen typed definition attributes without config reads", async () => {
  const tracer = new RecordingTracer();
  const environment = makeTestEnvironment([], { tracing: tracing(tracer) });
  // Deliberately absent from runtime config: cached identity is sufficient.
  const stream = new ObservedStream(config, environment);
  const context = new SamplingContext();
  context.sampled = true;
  for (let i = 0; i < 3; i += 1) await stream.deliver(context, noop);
  const attributes = tracer.attributes[0];
  assert.ok(attributes);
  assert.deepEqual(attributes, [
    { key: "stream", type: "string", value: "Price" },
    { key: "pipeline", type: "string", value: "pricing" },
    { key: "component", type: "string", value: "Customer Pricing" }
  ]);
  assert.ok(Object.isFrozen(attributes));
  for (const attribute of attributes) assert.ok(Object.isFrozen(attribute));
  for (const reused of tracer.attributes) assert.equal(reused, attributes);
  context.sampled = false;
  await stream.deliver(context, noop);
  assert.equal(tracer.attributes.length, 3, "Sampling remains live rather than cached");
  assert.equal(tracer.ended, 3);
});

await test("every gRPC output span is guarded at the call site before argument evaluation", async () => {
  const source = await readFile(
    new URL("../../src/datasink/grpc/grpc-js.ts", import.meta.url),
    "utf8"
  );
  const file = ts.createSourceFile("grpc-js.ts", source, ts.ScriptTarget.Latest, true);
  let spans = 0;
  function visit(node: ts.Node): void {
    if (
      ts.isCallExpression(node) &&
      ts.isPropertyAccessExpression(node.expression) &&
      node.expression.name.text === "start" &&
      node.arguments.some(
        (argument) => ts.isStringLiteral(argument) && argument.text === "grpc.output"
      )
    ) {
      spans += 1;
      let parent = parentOf(node);
      let guarded = false;
      while (parent !== undefined && !ts.isMethodDeclaration(parent)) {
        const condition = ts.isIfStatement(parent)
          ? parent.expression
          : ts.isConditionalExpression(parent) && parent.whenTrue === node
            ? parent.condition
            : undefined;
        if (condition?.getText(file) === "this.#tracer !== undefined && context.samplingEnabled()")
          guarded = true;
        parent = parentOf(parent);
      }
      assert.ok(guarded, "Span arguments must be behind the tracer and live sampling guard");
    }
    ts.forEachChild(node, visit);
  }
  visit(file);
  assert.equal(spans, 4, "Unary and all three streaming paths are covered");
});
