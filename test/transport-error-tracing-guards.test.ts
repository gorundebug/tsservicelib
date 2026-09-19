import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import ts from "typescript";
import {
  DurableCallContext,
  MessageContext,
  SpanStatusCode,
  type DurableCallEvent
} from "@gorundebug/tsservicelib/runtime";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";

const transportPaths = [
  "datasource/http/node-http.ts",
  "datasink/http/node-http.ts",
  "datasource/grpc/grpc-js.ts",
  "datasink/grpc/grpc-js.ts",
  "datasource/kafka/confluent.ts",
  "datasink/kafka/confluent.ts",
  "datasource/localsource/custom.ts",
  "datasink/localsink/custom.ts",
  "datasource/temporal/temporal.ts",
  "datasink/temporal/temporal.ts",
  "runtime/durable-call-context.ts"
];

function parentOf(node: ts.Node): ts.Node | undefined {
  return ts.isSourceFile(node) ? undefined : node.parent;
}

function hasSpanGuard(condition: ts.Expression, span: ts.Expression, file: ts.SourceFile): boolean {
  if (!ts.isBinaryExpression(condition)) return false;
  if (condition.operatorToken.kind === ts.SyntaxKind.AmpersandAmpersandToken) {
    return hasSpanGuard(condition.left, span, file) || hasSpanGuard(condition.right, span, file);
  }
  return (
    condition.operatorToken.kind === ts.SyntaxKind.ExclamationEqualsEqualsToken &&
    condition.left.getText(file) === span.getText(file) &&
    ts.isIdentifier(condition.right) &&
    condition.right.text === "undefined"
  );
}

for (const path of transportPaths) {
  await test(`error tracing is guarded before evaluating arguments: ${path}`, async () => {
    const source = await readFile(new URL(`../../src/${path}`, import.meta.url), "utf8");
    const file = ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true);
    let calls = 0;
    function visit(node: ts.Node): void {
      if (
        ts.isCallExpression(node) &&
        ts.isIdentifier(node.expression) &&
        node.expression.text === "spanError"
      ) {
        calls += 1;
        const span = node.arguments[0];
        assert.ok(span, "An explicit typed span must be passed");
        let child: ts.Node = node;
        let parent = parentOf(node);
        let guarded = false;
        while (parent !== undefined) {
          if (
            ts.isIfStatement(parent) &&
            parent.thenStatement === child &&
            hasSpanGuard(parent.expression, span, file)
          )
            guarded = true;
          child = parent;
          parent = parentOf(parent);
        }
        const position = file.getLineAndCharacterOfPosition(node.getStart(file));
        assert.ok(
          guarded,
          `${path}:${String(position.line + 1)} invokes spanError without a caller-side guard`
        );
      }
      ts.forEachChild(node, visit);
    }
    visit(file);
    assert.ok(calls > 0, "The test must cover actual error tracing call sites");
  });
}

await test("untraced durable errors do not read trace metadata but still report diagnostics", () => {
  let messageReads = 0;
  const failure = new Error("business failure");
  Object.defineProperty(failure, "message", {
    get: () => {
      messageReads += 1;
      return "business failure";
    }
  });
  const events: { readonly event: DurableCallEvent; readonly error: Error | undefined }[] = [];
  const durable = new DurableCallContext("request", "Activity", {
    diagnostics: (event, error) => {
      events.push({ event, error });
    }
  });
  durable.heartbeat("working");
  durable.close(failure);
  durable.close(failure);
  assert.equal(messageReads, 0, "No span means no reads to construct trace attributes");
  assert.deepEqual(
    events.map(({ event }) => event),
    ["heartbeat", "error"]
  );
  assert.equal(events[1]?.error, failure, "Business diagnostics retain the original error");
});

await test("traced durable errors retain lifecycle events, error status, and original attributes", () => {
  const tracing = new TestTracing();
  const started = tracing
    .tracer("service")
    .start(new MessageContext().withSampling(true), "temporal.activity");
  const durable = new DurableCallContext("request", "Activity");
  durable.bindSpan(started.span);
  durable.heartbeat("working");
  durable.close(new Error("business failure"));
  const [span] = tracing.spans();
  assert.ok(span);
  assert.equal(span.statusCode, SpanStatusCode.Error);
  assert.deepEqual(
    span.events.map(({ name }) => name),
    ["temporal.activity.heartbeat", "temporal.activity.error"]
  );
  assert.deepEqual(span.events[0]?.attributes, []);
  assert.deepEqual(span.events[1]?.attributes, [
    { key: "error", type: "string", value: "business failure" }
  ]);
});
