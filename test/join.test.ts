import assert from "node:assert/strict";
import { test } from "node:test";

import { makeJoinStream, type JoinFunction } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  JoinStorageType,
  JoinType,
  MessageContext,
  ServiceStream,
  type JoinStreamConfig,
  type JoinStorage,
  type JoinStorageConfig,
  type KeyValue,
  type Stream,
  type StreamConfig,
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

function joinConfig(
  joinType: JoinType,
  joinStorage: JoinStorageType = JoinStorageType.HashMap
): JoinStreamConfig {
  return {
    id: 3,
    name: "ordersWithInventory",
    properties: {},
    type: "Join",
    pipeline: "main",
    idService: 1,
    idSource: 1,
    idSources: [2],
    xPos: 3,
    yPos: 0,
    valueType: "string",
    joinType,
    joinStorage,
    ttl: 0,
    renewTTL: false
  };
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

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

interface JoinCall {
  readonly key: string;
  readonly left: readonly number[];
  readonly right: readonly string[];
}

async function setup(joinType: JoinType, retire = true) {
  const leftConfig = sourceConfig(1, "left");
  const rightConfig = sourceConfig(2, "right");
  const config = joinConfig(joinType);
  const outputConfig = sourceConfig(4, "output");
  const environment = makeTestEnvironment([leftConfig, rightConfig, config, outputConfig]);
  const left = new ConsumedStream<KeyValue<string, number>>(
    leftConfig,
    environment,
    makeTestSerde()
  );
  const right = new ConsumedStream<KeyValue<string, string>>(
    rightConfig,
    environment,
    makeTestSerde()
  );
  const calls: JoinCall[] = [];
  const function_: JoinFunction<string, number, string, string> = {
    async join(context, stream, key, leftValues, rightValues, out): Promise<boolean> {
      assert.equal(stream.name, "ordersWithInventory");
      calls.push({ key, left: [...leftValues], right: [...rightValues] });
      await out.out(context, `${key}:${leftValues.join(",")}:${rightValues.join(",")}`);
      return retire;
    }
  };
  const join = makeJoinStream(config, left, right, function_);
  await join.storage().start(Context.background());
  const output = new RecordingStream<string>(outputConfig, environment);
  join.setConsumer(output);
  return { calls, environment, join, left, output, right };
}

await test("inner join waits for both indexed sides and retires when function returns true", async () => {
  const { calls, environment, join, left, output, right } = await setup(JoinType.Inner);
  await left.emit(new MessageContext(), { key: "A", value: 1 });
  assert.deepEqual(calls, []);
  await right.emit(new MessageContext(), { key: "A", value: "stock" });

  assert.deepEqual(calls, [{ key: "A", left: [1], right: ["stock"] }]);
  assert.deepEqual(output.values, ["A:1:stock"]);
  assert.equal(join.storage().size(), 0);
  assert.deepEqual(environment.storages(), [join.storage()]);
});

await test("left, right and outer join gates match canonical side availability", async () => {
  const leftJoin = await setup(JoinType.Left);
  await leftJoin.left.emit(new MessageContext(), { key: "L", value: 1 });
  assert.deepEqual(leftJoin.calls, [{ key: "L", left: [1], right: [] }]);

  const rightJoin = await setup(JoinType.Right);
  await rightJoin.left.emit(new MessageContext(), { key: "R", value: 1 });
  assert.deepEqual(rightJoin.calls, []);
  await rightJoin.right.emit(new MessageContext(), { key: "R", value: "ready" });
  assert.deepEqual(rightJoin.calls, [{ key: "R", left: [1], right: ["ready"] }]);

  const outerJoin = await setup(JoinType.Outer);
  await outerJoin.right.emit(new MessageContext(), { key: "O", value: "only" });
  assert.deepEqual(outerJoin.calls, [{ key: "O", left: [], right: ["only"] }]);
});

await test("join retains accumulated values while function returns false", async () => {
  const { calls, join, left, right } = await setup(JoinType.Inner, false);
  await left.emit(new MessageContext(), { key: "A", value: 1 });
  await right.emit(new MessageContext(), { key: "A", value: "first" });
  await right.emit(new MessageContext(), { key: "A", value: "second" });

  assert.deepEqual(calls, [
    { key: "A", left: [1], right: ["first"] },
    { key: "A", left: [1], right: ["first", "second"] }
  ]);
  assert.equal(join.storage().size(), 1);
});

await test("join uses the service extension storage before the canonical fallback", () => {
  const leftConfig = sourceConfig(1, "left");
  const rightConfig = sourceConfig(2, "right");
  const config = joinConfig(JoinType.Inner, JoinStorageType.RocksDB);
  const seen: { storageType?: JoinStorageType; config?: JoinStorageConfig; stream?: Stream } = {};
  let customStorage: JoinStorage<unknown> | undefined;
  const environment = makeTestEnvironment([leftConfig, rightConfig, config], {
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
  const left = new ConsumedStream<KeyValue<string, number>>(
    leftConfig,
    environment,
    makeTestSerde()
  );
  const right = new ConsumedStream<KeyValue<string, string>>(
    rightConfig,
    environment,
    makeTestSerde()
  );
  const join = makeJoinStream(config, left, right, {
    join(): boolean {
      return true;
    }
  });

  assert.equal(join.storage(), customStorage);
  assert.equal(seen.storageType, JoinStorageType.RocksDB);
  assert.equal(seen.config?.name(), "ordersWithInventory");
  assert.equal(seen.stream, join);
  assert.deepEqual(environment.storages(), [customStorage]);
});
