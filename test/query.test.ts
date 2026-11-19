import assert from "node:assert/strict";
import { test } from "node:test";

import { makeQueryStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  MessageContext,
  ServiceStream,
  stringSerdeType,
  type Consumer,
  type QueryStreamConfig,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";

import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

function sourceConfig(id: number): StreamConfig {
  return {
    id,
    name: `source-${String(id)}`,
    properties: {},
    type: "Map",
    pipeline: "main",
    idService: 1,
    idSource: 0,
    idSources: [],
    xPos: 0,
    yPos: 0
  };
}

function queryConfig(id: number, source: number): QueryStreamConfig {
  return {
    id,
    name: `sink-${String(id)}`,
    properties: {},
    type: "Query",
    pipeline: "main",
    idService: 1,
    idSource: source,
    idSources: [],
    xPos: 1,
    yPos: 0,
    idEndpoint: 100,
    valueType: "int32"
  };
}

function resultConfig(id: number, source: number): StreamConfig {
  return {
    ...sourceConfig(id),
    name: `result-${String(id)}`,
    type: "Map",
    idSource: source
  };
}

function environment(configs: readonly StreamConfig[]) {
  return makeTestEnvironment(configs, {
    dataConnectors: [{ id: 10, name: "grpc", type: 4, properties: {}, implementation: "grpc" }],
    endpoints: [{ id: 100, name: "remote", properties: {}, idDataConnector: 10 }]
  });
}

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

await test("Query supports zero or many results and independent errors without an implicit completion", async () => {
  const sourceCfg = sourceConfig(10);
  const queryCfg = queryConfig(11, 10);
  const resultCfg = resultConfig(12, 11);
  const errorCfg = resultConfig(13, 11);
  const env = environment([sourceCfg, queryCfg, resultCfg, errorCfg]);
  const errorType = registerTestSerdeType(env, "test.QueryError", (value): value is Error => value instanceof Error);
  env.serdeRegistry().registerStreamErrorType(queryCfg.id, errorType);
  const source = new ConsumedStream(sourceCfg, env, env.serde(stringSerdeType));
  const query = makeQueryStream<string, number, Error>(queryCfg, source);
  const results = new RecordingStream<number>(resultCfg, env);
  const errors = new RecordingStream<Error>(errorCfg, env);
  query.setConsumer(results);
  query.errorStream().setConsumer(errors);
  const failure = new Error("query failed after a partial result");
  const endpoint: Consumer<string> = {
    async consume(context, value): Promise<void> {
      if (value === "empty") return;
      await query.consumeResult(context, 1);
      if (value === "fail") {
        await query.errorStream().emit(context, failure);
        return;
      }
      await query.consumeResult(context, 2);
    }
  };
  query.setQueryConsumer(endpoint);
  await source.emit(new MessageContext(), "empty");
  assert.deepEqual(results.values, []);
  await source.emit(new MessageContext(), "many");
  assert.deepEqual(results.values, [1, 2]);
  await source.emit(new MessageContext(), "fail");
  assert.deepEqual(results.values, [1, 2, 1]);
  assert.deepEqual(errors.values, [failure]);
  assert.equal(query.endpointId(), 100);
  assert.equal(query.errorStream().id, -query.id);
});

await test("Query lifecycle passes the ordinary out to endRequest, including failures and cancellation", async () => {
  const { makeEndpointConsumer } = await import("@gorundebug/tsservicelib/dataquery");
  const failure = new Error("storage unavailable");
  for (const mode of ["empty", "many", "partial", "begin", "cancel"] as const) {
    const sourceCfg = sourceConfig(10);
    const queryCfg = queryConfig(11, 10);
    const outputCfg = resultConfig(12, 11);
    const errorCfg = resultConfig(13, 11);
    const env = environment([sourceCfg, queryCfg, outputCfg, errorCfg]);
    env.serdeRegistry().registerStreamErrorType(queryCfg.id, registerTestSerdeType(env, "test.LifecycleError", (value): value is Error => value instanceof Error));
    const source = new ConsumedStream(sourceCfg, env, env.serde(stringSerdeType));
    const query = makeQueryStream<string, number, Error>(queryCfg, source);
    const results = new RecordingStream<number>(outputCfg, env);
    const failures = new RecordingStream<Error>(errorCfg, env);
    query.setConsumer(results);
    query.errorStream().setConsumer(failures);
    let ends = 0;
    let executions = 0;
    makeEndpointConsumer(query, {
      beginRequest(context) {
        return { context, state: 41, ...(mode === "begin" ? { error: failure } : {}) };
      },
      async endRequest(context, _stream, state, error, out, errors) {
        ends++;
        assert.equal(state, 41);
        if (error !== undefined) await errors.out(context, error);
        await out.out(context, 99); // Application-defined completion, not a special port.
      }
    }, async (context, state, value, out) => {
      executions++;
      assert.equal(state, 41);
      assert.equal(value, "request");
      if (mode === "empty") return;
      await out.out(context, 1);
      if (mode === "partial") throw failure;
      await out.out(context, 2);
    });
    const abort = new AbortController();
    if (mode === "cancel") abort.abort(failure);
    await source.emit(new MessageContext(abort.signal), "request");
    assert.equal(ends, 1, mode);
    assert.equal(executions, mode === "begin" || mode === "cancel" ? 0 : 1, mode);
    assert.deepEqual(results.values, mode === "many" ? [1, 2, 99] : mode === "partial" ? [1, 99] : [99], mode);
    assert.deepEqual(failures.values, mode === "partial" || mode === "begin" || mode === "cancel" ? [failure] : [], mode);
  }
});

await test("disabled Query endpoint does not invoke its handler", async () => {
  const sourceCfg = sourceConfig(10);
  const queryCfg = queryConfig(11, 10);
  const env = makeTestEnvironment([sourceCfg, queryCfg], {
    dataConnectors: [{ id: 10, name: "custom", type: 4, properties: {}, implementation: "custom" }],
    endpoints: [{ ...{ id: 100, name: "disabled", properties: {}, idDataConnector: 10, enabled: false } }]
  });
  const errorType = registerTestSerdeType(env, "test.DisabledQueryError", (value): value is Error => value instanceof Error);
  env.serdeRegistry().registerStreamErrorType(queryCfg.id, errorType);
  const source = new ConsumedStream(sourceCfg, env, env.serde(stringSerdeType));
  const query = makeQueryStream<string, number, Error>(queryCfg, source);
  query.setQueryConsumer({ consume() { assert.fail("disabled handler called"); } });
  await source.emit(new MessageContext(), "skip");
});

await test("Query completion precedes queued downstream completion", {timeout: 3000}, async () => {
  const { makeEndpointConsumer } = await import("@gorundebug/tsservicelib/dataquery");
  const { RuntimeCallerFactory, RuntimeTaskRegistry, TaskPool } = await import("@gorundebug/tsservicelib/runtime");
  const inputCfg = sourceConfig(10), queryCfg = queryConfig(11, 10), outputCfg = resultConfig(12, 11);
  const pool = new TaskPool({name: "query-results", executorsCount: 1});
  const pools = new Map([[pool.name(), pool]]);
  const factory = new RuntimeCallerFactory({config: () => env.runtimeConfig(), serviceId: 1,
    taskPools: pools, priorityTaskPools: new Map(), tasks: new RuntimeTaskRegistry()});
  const env = makeTestEnvironment([inputCfg, queryCfg, outputCfg], {
    callerFactory: factory, taskPools: pools,
    pools: [{name: pool.name(), executorsCount: 1, queueCapacity: 0, properties: {}}],
    links: [{from: 11, to: 12, properties: {}, callSemantics: {taskPool: {poolName: pool.name()}}}],
    dataConnectors: [{id: 10, name: "database", type: 4, properties: {}, implementation: "custom"}],
    endpoints: [{id: 100, name: "rows", properties: {}, idDataConnector: 10}]
  });
  env.serdeRegistry().registerStreamErrorType(queryCfg.id,
    registerTestSerdeType(env, "test.QueuedQueryError", (value): value is Error => value instanceof Error));
  const source = new ConsumedStream(inputCfg, env, env.serde(stringSerdeType));
  const query = makeQueryStream<string, number, Error>(queryCfg, source);
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>(resolve => {release = resolve;});
  const started = new Promise<void>(resolve => {entered = resolve;});
  const values: number[] = [];
  class SlowResult extends ServiceStream implements TypedStreamConsumer<number> {
    async consume(_ctx: MessageContext, value: number): Promise<void> {
      entered(); await gate; values.push(value);
    }
  }
  query.setConsumer(new SlowResult(outputCfg, env));
  let ends = 0;
  makeEndpointConsumer(query, {
    beginRequest: context => ({context, state: null}),
    async endRequest(context, _stream, _state, error, out) {
      assert.equal(error, undefined); ends++; await out.out(context, 99);
    }
  }, async (context, _state, _input, out) => {await out.out(context, 42);});
  const context = new MessageContext();
  await pool.start(context);
  try {
    await source.emit(context, "request");
    await started;
    assert.equal(ends, 1);
    assert.deepEqual(values, []);
  } finally {
    release(); await pool.stop(context);
  }
  assert.deepEqual(values, [42, 99]);
});
