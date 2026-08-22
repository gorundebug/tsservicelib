import assert from "node:assert/strict";
import { test } from "node:test";

import { makeProcessStream, type ProcessFunction } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  makeStatusNetworkData,
  runtimeToCanonicalConfig,
  MessageContext,
  ServiceStream,
  TaskPool,
  int32SerdeType,
  stringSerdeType,
  type Completion,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

function config<const T extends StreamConfig["type"]>(
  id: number,
  name: string,
  type: T,
  source = 0
): StreamConfig & { readonly type: T } {
  return {
    id,
    name,
    type,
    pipeline: "main",
    idService: 1,
    idSource: source,
    idSources: [],
    xPos: id * 10,
    yPos: -id,
    properties: {}
  };
}

class Terminal<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
  public consume(context: MessageContext, value: T): void {
    void context;
    void value;
  }
}

class TerminalSink<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public consume(context: MessageContext, value: T): void {
    void context;
    void value;
  }
}

await test("status graph uses live caller and virtual error-link counters", async () => {
  const sourceConfig = config(1, "Input", "Input");
  const processConfig = { ...config(2, "Process", "Process", 1) } as const;
  const resultConfig = config(3, "Result", "Sink", 2);
  const errorConfig = config(4, "Errors", "Sink", 2);
  const environment = makeTestEnvironment(
    [sourceConfig, processConfig, resultConfig, errorConfig],
    { service: { name: "Service", color: "#123456" } }
  );
  environment.serdeRegistry().registerStreamValueType(processConfig.id, int32SerdeType);
  environment.serdeRegistry().registerStreamErrorType(processConfig.id, stringSerdeType);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  environment.registerStream(source);
  const function_: ProcessFunction<number, number, string> = {
    process(context, _stream, value, out, errorOut): Completion {
      return value >= 0 ? out.out(context, value) : errorOut.out(context, String(value));
    }
  };
  const process = makeProcessStream(processConfig, source, function_);
  const result = new Terminal(resultConfig, environment, environment.serde(int32SerdeType));
  const errors = new Terminal(errorConfig, environment, environment.serde(stringSerdeType));
  environment.registerStream(result);
  environment.registerStream(errors);
  process.setConsumer(result);
  process.errorStream().setConsumer(errors);

  await source.emit(new MessageContext(), 1);
  await source.emit(new MessageContext(), -1);

  const graph = makeStatusNetworkData(environment);
  assert.deepEqual(
    graph.nodes.map(({ id, label, x, y }) => ({ id, label, x, y })),
    [
      { id: 1, label: "Input(INPUT)\n[Service]", x: 10, y: -1 },
      { id: 2, label: "Process(PROCESS)\n[Service]", x: 20, y: -2 },
      { id: 3, label: "Result(SINK)\n[Service]", x: 30, y: -3 },
      { id: 4, label: "Errors(SINK)\n[Service]", x: 40, y: -4 },
      { id: -2, label: "Process Error(ERROR)\n[Service]", x: 0, y: 0 }
    ]
  );
  assert.deepEqual(
    graph.edges.map(({ from, to, label, color }) => ({ from, to, label, color: color.color })),
    [
      { from: 1, to: 2, label: "int32\ncalls: 2", color: "#0050FF" },
      { from: 2, to: 3, label: "int32\ncalls: 1", color: "#0050FF" },
      { from: 2, to: -2, label: "int32\ncalls: 1", color: "#FF3030" },
      { from: -2, to: 4, label: "string\ncalls: 1", color: "#0050FF" }
    ]
  );
  assert.match(graph.nodes[0]?.image.unselected ?? "", /^data:image\/svg\+xml/);

  const runtimeConfig = runtimeToCanonicalConfig(environment);
  assert.deepEqual(
    runtimeConfig.streams.map(({ id }) => id),
    [1, 2, 3, 4, -2]
  );
  assert.deepEqual(
    runtimeConfig.streams.find(({ id }) => id === -2),
    {
      id: -2,
      name: "Process Error",
      properties: {},
      type: "Error",
      pipeline: "main",
      idService: 1,
      idSource: 2,
      idSources: [],
      xPos: 0,
      yPos: 0,
      valueType: "string"
    }
  );
  assert.equal(runtimeConfig.streams.find(({ id }) => id === 4)?.idSource, -2);
});

await test("status graph includes a terminal sink referenced by an incoming edge", () => {
  const sourceConfig = config(1, "Input", "Input");
  const sinkConfig = config(2, "Terminal", "Sink", 1);
  const environment = makeTestEnvironment([sourceConfig, sinkConfig], {
    service: { name: "Service", color: "#123456" }
  });
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  environment.registerStream(source);
  const sink = new TerminalSink<number>(sinkConfig, environment);
  source.setConsumer(sink);
  environment.registerStream(sink);

  const graph = makeStatusNetworkData(environment);
  assert.deepEqual(
    graph.nodes.map(({ id }) => id),
    [1, 2]
  );
  assert.deepEqual(
    graph.edges.map(({ from, to }) => ({ from, to })),
    [{ from: 1, to: 2 }]
  );
  assert.ok(
    graph.edges.every(
      ({ from, to }) =>
        graph.nodes.some(({ id }) => id === from) && graph.nodes.some(({ id }) => id === to)
    )
  );
});

await test("runtime graph reports the live pool size and registered streams only", () => {
  const sourceConfig = config(1, "Input", "Input");
  const unregisteredConfig = config(2, "NotBuilt", "Map", 1);
  const pool = new TaskPool({ name: "work", executorsCount: 2 });
  pool.resize(5);
  const environment = makeTestEnvironment([sourceConfig, unregisteredConfig], {
    pools: [{ name: "work", executorsCount: 2, queueCapacity: 0, properties: {} }],
    taskPools: new Map([["work", pool]])
  });
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  environment.registerStream(source);

  const runtimeConfig = runtimeToCanonicalConfig(environment);
  assert.deepEqual(
    runtimeConfig.streams.map(({ id }) => id),
    [1]
  );
  assert.equal(runtimeConfig.pools[0]?.executorsCount, 5);
});
