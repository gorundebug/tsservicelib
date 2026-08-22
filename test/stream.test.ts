import assert from "node:assert/strict";
import { test } from "node:test";

import {
  FunctionCaller,
  makeCollector,
  MessageContext,
  ServiceStream,
  type Consumer,
  type StreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

const streamConfig: StreamConfig = {
  id: 10,
  name: "processOrder",
  properties: {},
  type: "Input",
  pipeline: "main",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0
};

await test("FunctionCall async metadata does not detach direct delivery", async () => {
  const events: string[] = [];
  const consumer: Consumer<string> = {
    async consume(_context, value): Promise<void> {
      events.push(`start:${value}`);
      await Promise.resolve();
      events.push("done");
    }
  };
  const caller = new FunctionCaller(consumer, true);
  const completion = caller.consume(new MessageContext(), "message");

  assert.equal(caller.isAsync(), true);
  assert.deepEqual(events, ["start:message"]);
  await completion;
  assert.deepEqual(events, ["start:message", "done"]);
});

await test("collector forwards the exact context and value reference without copying", async () => {
  const context = new MessageContext().withStreamId("request-1");
  const value = { id: 1 };
  let actualContext: MessageContext | undefined;
  let actualValue: { id: number } | undefined;
  const caller = new FunctionCaller<{ id: number }>({
    consume(receivedContext, receivedValue): void {
      actualContext = receivedContext;
      actualValue = receivedValue;
    }
  });

  await makeCollector(caller).out(context, value);
  assert.equal(actualContext, context);
  assert.equal(actualValue, value);
});

await test("service stream stores immutable graph identity and no config snapshot", () => {
  const stream = new ServiceStream(streamConfig, makeTestEnvironment([streamConfig]));

  assert.deepEqual(
    {
      id: stream.id,
      name: stream.name,
      transformationName: stream.transformationName
    },
    {
      id: 10,
      name: "processOrder",
      transformationName: "input"
    }
  );
  assert.equal(stream.config(), streamConfig);
});
