import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";

import type * as ConfluentKafka from "@confluentinc/kafka-javascript";
import {
  makeKafkaEndpointConsumer,
  type ConsumerMessage,
  type EndpointHandler
} from "@gorundebug/tsservicelib/datasource/kafka";
import { makeInputStream } from "@gorundebug/tsservicelib/operators";
import {
  Context,
  ServiceStream,
  errorSerdeType,
  type InputStreamConfig,
  type KafkaDataConnectorConfig,
  type KafkaEndpointConfig,
  type MessageContext,
  type StreamContext,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

class Handler implements EndpointHandler<undefined, string, string, Error> {
  public readonly errors: Error[] = [];
  public concurrency(): number {
    return 2;
  }
  public beginRequest(context: MessageContext): { context: MessageContext; state: undefined } {
    return { context, state: undefined };
  }
  public async consumeMessage(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    _state: undefined,
    message: ConsumerMessage
  ): Promise<void> {
    await stream.collect(context, Buffer.from(message.value).toString("utf8"));
    message.markMessage();
  }
  public getMessageId(
    _context: MessageContext,
    _stream: StreamContext<string, string, Error>,
    _state: undefined,
    value: string
  ): string {
    return value;
  }
  public onError(
    _context: MessageContext,
    _stream: StreamContext<string, string, Error>,
    error: Error
  ): void {
    this.errors.push(error);
  }
  public endRequest(
    _context: MessageContext,
    _stream: StreamContext<string, string, Error>,
    error: Error | undefined
  ): void {
    if (error !== undefined) this.errors.push(error);
  }
}

class Observation extends ServiceStream implements TypedStreamConsumer<string> {
  public readonly values: string[] = [];
  public consume(_context: MessageContext, value: string): void {
    this.values.push(value);
  }
}

const brokers = process.env["SA_TEST_KAFKA_BROKERS"];
await test(
  "native Kafka endpoint hides aborted/open transactions and accepts producer reuse",
  {
    skip: brokers === undefined ? "set SA_TEST_KAFKA_BROKERS for native Kafka acceptance" : false,
    timeout: 90_000
  },
  async (t) => {
    assert.ok(brokers);
    // Load the real native binding only for explicitly enabled broker acceptance.
    const { KafkaJS } = createRequire(import.meta.url)(
      "@confluentinc/kafka-javascript"
    ) as typeof ConfluentKafka;
    const topic = `sa-ts-visibility-${randomUUID()}`;
    const kafka = new KafkaJS.Kafka({
      kafkaJS: { brokers: brokers.split(","), logLevel: KafkaJS.logLevel.ERROR }
    });
    const ordinary = kafka.producer();
    const transactional = kafka.producer({
      kafkaJS: { transactionalId: topic, transactionTimeout: 30_000 }
    });
    const inputConfig: InputStreamConfig = {
      id: 1,
      name: "input",
      properties: {},
      type: "Input",
      pipeline: "main",
      idService: 1,
      idSource: 0,
      idSources: [],
      xPos: 0,
      yPos: 0,
      idEndpoint: 100,
      valueType: "string"
    };
    const observedConfig = {
      ...inputConfig,
      id: 2,
      name: "observed",
      type: "Map" as const,
      idSource: 1
    };
    const connector: KafkaDataConnectorConfig = {
      id: 10,
      name: "events",
      type: 3,
      implementation: "confluent/kafka-javascript",
      properties: {},
      brokers,
      dialTimeout: 10_000,
      usePartitioner: false,
      async: true,
      securityProtocol: "PLAINTEXT",
      saslMechanism: "PLAIN"
    };
    const endpoint: KafkaEndpointConfig = {
      id: 100,
      name: "events",
      idDataConnector: 10,
      properties: {},
      enabled: true,
      createTopic: true,
      topic,
      partitions: 2,
      consumerGroup: topic,
      replicationFactor: 1
    };
    const environment = makeTestEnvironment([inputConfig, observedConfig], {
      dataConnectors: [connector],
      endpoints: [endpoint]
    });
    environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
    const input = makeInputStream<string, string, Error>(inputConfig, environment);
    const observation = new Observation(observedConfig, environment);
    input.setConsumer(observation);
    const handler = new Handler();
    makeKafkaEndpointConsumer(input, handler); // Default native factory, no injected transport.
    const source = environment.dataSourceById(10);
    assert.ok(source);
    const context = new Context(t.signal);
    t.after(async () => {
      await transactional.disconnect();
      await ordinary.disconnect();
      await source.stop(context.withoutCancellation().bounded(10_000));
    });
    await source.start(context);
    await ordinary.connect();
    await transactional.connect();
    const expected: string[] = [];
    async function expect(...added: string[]): Promise<void> {
      expected.push(...added);
      const deadline = Date.now() + 10_000;
      while (observation.values.length < expected.length && Date.now() < deadline) {
        assert.deepEqual(handler.errors, []);
        await delay(10, undefined, { signal: t.signal });
      }
      assert.deepEqual(observation.values, expected);
      assert.deepEqual(handler.errors, []);
    }
    async function send(
      producer: ConfluentKafka.KafkaJS.Producer,
      value: string,
      partition = 0
    ): Promise<void> {
      await producer.send({ topic, messages: [{ value, partition }] });
    }
    await send(ordinary, "baseline");
    await expect("baseline");
    let tx = await transactional.transaction();
    await send(tx, "aborted");
    await tx.abort();
    await send(ordinary, "after-abort");
    await expect("after-abort");

    tx = await transactional.transaction();
    await send(tx, "open-then-committed");
    await send(ordinary, "behind-open");
    await send(ordinary, "other-partition", 1);
    await expect("other-partition");
    await delay(500, undefined, { signal: t.signal });
    assert.deepEqual(observation.values, expected);
    await tx.commit();
    await expect("open-then-committed", "behind-open");

    tx = await transactional.transaction();
    await send(tx, "aborted-again");
    await tx.abort();
    tx = await transactional.transaction();
    await send(tx, "reused-producer-commit");
    await tx.commit();
    await send(ordinary, "final-barrier");
    await expect("reused-producer-commit", "final-barrier");
  }
);
