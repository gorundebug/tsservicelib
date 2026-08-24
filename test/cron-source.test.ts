import assert from "node:assert/strict";
import { test } from "node:test";

import { Cron } from "croner";

import { makeCronEndpointConsumer } from "@gorundebug/tsservicelib/datasource/cron";
import { InputStream } from "@gorundebug/tsservicelib/operators";
import {
  Context,
  type Collector,
  DataConnectorType,
  ScheduleBackend,
  ServiceStream,
  type CronEndpointConfig,
  type InputStreamConfig,
  type MessageContext,
  type ScheduleEndpointFunction,
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
  public readonly received = Promise.withResolvers<string>();

  public consume(_context: MessageContext, value: string): void {
    this.received.resolve(value);
  }
}

const function_: ScheduleEndpointFunction<string> = {
  onTrigger(context: MessageContext, trigger: Readonly<ScheduleTrigger>, out: Collector<string>) {
    return out.out(context, `${trigger.scheduleId}:${trigger.backend}`);
  }
};

await test("Croner datasource invokes the endpoint function before the input stream", async () => {
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
  const input = new InputStream<string, never, Error>(
    streamConfig,
    environment,
    makeTestSerde(),
    makeTestSerde()
  );
  const consumer = new TriggerConsumer(consumerConfig, environment);
  input.setConsumer(consumer);
  makeCronEndpointConsumer(input, function_);
  const dataSource = environment.dataSourceById(10);
  assert.ok(dataSource);

  await dataSource.start(Context.background());
  const value = await Promise.race([
    consumer.received.promise,
    new Promise<never>((_resolve, reject) => {
      setTimeout(() => {
        reject(new Error("cron trigger timeout"));
      }, 2_500).unref();
    })
  ]);
  await dataSource.stop(Context.background());

  assert.equal(value, `${endpoint.name}:${ScheduleBackend.Local}`);
});

await test("Croner evaluates the portable schedule in UTC", () => {
  const job = new Cron("30 2 * * *", {
    paused: true,
    timezone: "UTC"
  });
  const next = job.nextRun(new Date("2026-03-07T03:00:00Z"));
  assert.ok(next);
  assert.equal(next.toISOString(), "2026-03-08T02:30:00.000Z");
});
