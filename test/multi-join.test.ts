import assert from "node:assert/strict";
import { test } from "node:test";

import {
  makeMultiJoinLink,
  makeMultiJoinStream,
  type MultiJoinFunction
} from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  JoinStorageType,
  MessageContext,
  ServiceStream,
  type KeyValue,
  type JoinStorage,
  type JoinStorageConfig,
  type MultiJoinStreamConfig,
  type StreamConfig,
  type Stream,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, makeTestSerde } from "./support/environment.js";

function sourceConfig(id: number, name: string): StreamConfig {
  return {
    id,
    name,
    properties: {},
    type: "Map",
    pipeline: "main",
    idService: 1,
    idSource: 0,
    idSources: [],
    xPos: id,
    yPos: 0
  };
}

const multiJoinConfig: MultiJoinStreamConfig = {
  id: 4,
  name: "aggregate",
  properties: {},
  type: "MultiJoin",
  pipeline: "main",
  idService: 1,
  idSource: 1,
  idSources: [2, 3],
  xPos: 4,
  yPos: 0,
  valueType: "string",
  joinStorage: JoinStorageType.HashMap,
  ttl: 0,
  renewTTL: false
};

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

class TestJoinStorage<K> implements JoinStorage<K> {
  readonly #values = new Map<K, unknown[][]>();
  public lifecycleCalls = 0;

  public start(): void {
    this.lifecycleCalls += 1;
  }

  public stop(): void {
    this.lifecycleCalls += 1;
  }

  public async joinValue(
    _context: MessageContext,
    key: K,
    index: number,
    value: unknown,
    callback: (values: unknown[][]) => boolean | Promise<boolean>
  ): Promise<void> {
    const values = this.#values.get(key) ?? [];
    (values[index] ??= []).push(value);
    this.#values.set(key, values);
    if (await callback(values)) this.#values.delete(key);
  }

  public size(): number {
    return this.#values.size;
  }
}

await test("multi-join preserves root slot zero and right-link insertion order", async () => {
  const rootConfig = sourceConfig(1, "root");
  const priceConfig = sourceConfig(2, "price");
  const stockConfig = sourceConfig(3, "stock");
  const outputConfig = sourceConfig(5, "output");
  const environment = makeTestEnvironment([
    rootConfig,
    priceConfig,
    stockConfig,
    multiJoinConfig,
    outputConfig
  ]);
  const root = new ConsumedStream<KeyValue<string, { order: number }>>(
    rootConfig,
    environment,
    makeTestSerde()
  );
  const price = new ConsumedStream<KeyValue<string, number>>(
    priceConfig,
    environment,
    makeTestSerde()
  );
  const stock = new ConsumedStream<KeyValue<string, boolean>>(
    stockConfig,
    environment,
    makeTestSerde()
  );
  const calls: (readonly (readonly unknown[])[])[] = [];
  const function_: MultiJoinFunction<string, { order: number }, string> = {
    async multiJoin(context, stream, key, values, out): Promise<boolean> {
      assert.equal(stream.name, "aggregate");
      calls.push(values.map((slot) => [...slot]));
      await out.out(context, key);
      return false;
    }
  };
  const multiJoin = makeMultiJoinStream(multiJoinConfig, root, function_);
  await multiJoin.storage().start(Context.background());
  makeMultiJoinLink(multiJoin, price);
  makeMultiJoinLink(multiJoin, stock);
  const output = new RecordingStream<string>(outputConfig, environment);
  multiJoin.setConsumer(output);

  await stock.emit(new MessageContext(), { key: "A", value: true });
  await price.emit(new MessageContext(), { key: "A", value: 99 });
  assert.deepEqual(calls, []);
  const order = { order: 1 };
  await root.emit(new MessageContext(), { key: "A", value: order });

  assert.deepEqual(calls, [[[order], [99], [true]]]);
  assert.deepEqual(output.values, ["A"]);
  assert.equal(multiJoin.storage().size(), 1);
});

await test("failed right binding does not consume a multi-join slot", async () => {
  const rootConfig = sourceConfig(1, "root");
  const occupiedConfig = sourceConfig(2, "occupied");
  const nextConfig = sourceConfig(3, "next");
  const environment = makeTestEnvironment([
    rootConfig,
    occupiedConfig,
    nextConfig,
    multiJoinConfig
  ]);
  const root = new ConsumedStream<KeyValue<string, number>>(
    rootConfig,
    environment,
    makeTestSerde()
  );
  const occupied = new ConsumedStream<KeyValue<string, string>>(
    occupiedConfig,
    environment,
    makeTestSerde()
  );
  const next = new ConsumedStream<KeyValue<string, boolean>>(
    nextConfig,
    environment,
    makeTestSerde()
  );
  const blocker = new RecordingStream<KeyValue<string, string>>(occupiedConfig, environment);
  occupied.setConsumer(blocker);
  let observed: readonly (readonly unknown[])[] | undefined;
  const multiJoin = makeMultiJoinStream(multiJoinConfig, root, {
    multiJoin(_context, _stream, _key, values): boolean {
      observed = values.map((slot) => [...slot]);
      return false;
    }
  });
  await multiJoin.storage().start(Context.background());

  assert.throws(() => {
    makeMultiJoinLink(multiJoin, occupied);
  }, /consumer already assigned/);
  makeMultiJoinLink(multiJoin, next);
  await next.emit(new MessageContext(), { key: "A", value: true });
  await root.emit(new MessageContext(), { key: "A", value: 1 });
  assert.deepEqual(observed, [[1], [true]]);
});

await test("multi-join uses the service extension storage before fallback", () => {
  const rootConfig = sourceConfig(1, "root");
  const config: MultiJoinStreamConfig = {
    ...multiJoinConfig,
    idSources: [],
    joinStorage: JoinStorageType.Aerospike
  };
  const seen: { storageType?: JoinStorageType; config?: JoinStorageConfig; stream?: Stream } = {};
  let customStorage: JoinStorage<unknown> | undefined;
  const environment = makeTestEnvironment([rootConfig, config], {
    joinStorageFactory: <K>(
      storageType: JoinStorageType,
      storageConfig: JoinStorageConfig,
      stream: Stream
    ) => {
      seen.storageType = storageType;
      seen.config = storageConfig;
      seen.stream = stream;
      const storage = new TestJoinStorage<K>();
      customStorage = storage;
      return storage;
    }
  });
  const root = new ConsumedStream<KeyValue<string, number>>(
    rootConfig,
    environment,
    makeTestSerde()
  );
  const multiJoin = makeMultiJoinStream(config, root, {
    multiJoin(): boolean {
      return false;
    }
  });

  assert.equal(multiJoin.storage(), customStorage);
  assert.equal(seen.storageType, JoinStorageType.Aerospike);
  assert.equal(seen.config?.name(), "aggregate");
  assert.equal(seen.stream, multiJoin);
  assert.deepEqual(environment.storages(), [customStorage]);
});
