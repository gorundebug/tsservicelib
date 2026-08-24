import assert from "node:assert/strict";
import { test } from "node:test";

import { Cron } from "croner";

import { makeCronEndpointConsumer } from "@gorundebug/tsservicelib/datasource/cron";
import { InputStream } from "@gorundebug/tsservicelib/operators";
import {
  Context,
  DataConnectorType,
  ScheduleBackend,
  ServiceStream,
  type CronEndpointConfig,
  type InputStreamConfig,
  type MessageContext,
  type ScheduleTrigger
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, makeTestSerde } from "./support/environment.js";

const streamConfig: InputStreamConfig = {
  id: 1,
  name: "everySecond",
  properties: {},
  type: "Input",
  pipeline: "scheduled",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0,
  idEndpoint: 100,
  valueType: "ScheduleTrigger"
};

const endpoint: CronEndpointConfig = {
  id: 100,
  name: "every-second",
  properties: {},
  idDataConnector: 10,
  enabled: true,
  schedule: "* * * * * *",
  timezone: "UTC",
  overlapPolicy: "Skip",
  missedRunPolicy: "FireOnce"
};

class TriggerConsumer extends ServiceStream {
  public readonly received = Promise.withResolvers<ScheduleTrigger>();

  public consume(_context: MessageContext, trigger: ScheduleTrigger): void {
    this.received.resolve(trigger);
  }
}

await test("Croner datasource directly activates the configured input stream", async () => {
  const consumerConfig = {
    ...streamConfig,
    id: 2,
    name: "consumeTrigger",
    type: "Map" as const,
    idSource: streamConfig.id
  };
  const environment = makeTestEnvironment([streamConfig, consumerConfig], {
    dataConnectors: [
      {
        id: 10,
        name: "localCron",
        properties: {},
        type: DataConnectorType.Cron,
        implementation: "node/croner"
      }
    ],
    endpoints: [endpoint]
  });
  const input = new InputStream<ScheduleTrigger, never, Error>(
    streamConfig,
    environment,
    makeTestSerde(),
    makeTestSerde()
  );
  const consumer = new TriggerConsumer(consumerConfig, environment);
  input.setConsumer(consumer);
  makeCronEndpointConsumer(input);
  const dataSource = environment.dataSourceById(10);
  assert.ok(dataSource);

  await dataSource.start(Context.background());
  const trigger = await Promise.race([
    consumer.received.promise,
    new Promise<never>((_resolve, reject) => {
      setTimeout(() => {
        reject(new Error("cron trigger timeout"));
      }, 2_500).unref();
    })
  ]);
  await dataSource.stop(Context.background());

  assert.equal(trigger.scheduleId, endpoint.name);
  assert.equal(trigger.backend, ScheduleBackend.Local);
  assert.match(trigger.triggerId, /^[0-9a-f]{64}$/u);
});

await test("Croner candidates satisfy the portable DST contract", () => {
  const spring = new Cron("30 2 * * *", {
    paused: true,
    timezone: "America/New_York"
  });
  const shifted = spring.nextRun(new Date("2026-03-07T08:00:00Z"));
  assert.ok(shifted);
  assert.equal(shifted.toISOString(), "2026-03-08T07:30:00.000Z");
  assert.equal(spring.match(shifted), false, "shifted gap candidate is filtered");

  const fall = new Cron("30 1 * * *", {
    paused: true,
    timezone: "America/New_York"
  });
  const first = fall.nextRun(new Date("2026-10-31T06:00:00Z"));
  assert.ok(first);
  const next = fall.nextRun(first);
  assert.ok(next);
  assert.equal(first.toISOString(), "2026-11-01T05:30:00.000Z");
  assert.equal(next.toISOString(), "2026-11-02T06:30:00.000Z");
});
