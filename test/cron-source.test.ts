import assert from "node:assert/strict";
import { test } from "node:test";

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
  valueType: "schedule trigger"
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
