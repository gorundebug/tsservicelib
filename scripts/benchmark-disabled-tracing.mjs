import { performance } from "node:perf_hooks";

import {
  ConsumedStream,
  MessageContext,
  RuntimeConfig,
  RuntimeConfigStore,
  ServiceEnvironment,
  int32SerdeType
} from "../dist/runtime/index.js";
import { makeMapStream } from "../dist/operators/index.js";

const iterations = Number.parseInt(process.env["ITERATIONS"] ?? "500000", 10);
const rounds = Number.parseInt(process.env["ROUNDS"] ?? "9", 10);
if (!Number.isSafeInteger(iterations) || iterations < 1) {
  throw new RangeError("ITERATIONS must be a positive safe integer");
}
if (!Number.isSafeInteger(rounds) || rounds < 3) {
  throw new RangeError("ROUNDS must be a safe integer of at least three");
}

const service = {
  id: 1,
  name: "benchmark",
  color: "#000000",
  properties: {},
  environment: "benchmark",
  grpcHost: "127.0.0.1",
  grpcPort: 9201,
  httpHost: "127.0.0.1",
  httpPort: 9091,
  metricsHandler: "/metrics",
  shutdownTimeout: 1_000,
  statusHandler: "/status"
};
const sourceConfig = {
  id: 1,
  name: "Input",
  type: "Input",
  pipeline: "main",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0,
  properties: {}
};
const mapConfig = {
  ...sourceConfig,
  id: 2,
  name: "Map",
  type: "Map",
  idSource: 1,
  valueType: "int32"
};

function makeHotPath(tracing) {
  const config = new RuntimeConfig({
    services: [service],
    streams: [sourceConfig, mapConfig],
    dataConnectors: [],
    endpoints: [],
    pools: [],
    links: [],
    modules: [],
    types: [],
    properties: {}
  });
  const environment = new ServiceEnvironment(
    new RuntimeConfigStore(config),
    service.id,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    tracing
  );
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  makeMapStream(mapConfig, source, {
    map(context, _stream, value, out) {
      return out.out(context, value);
    }
  });
  return source;
}

let enabledCalls = 0;
let tracerCalls = 0;
const disabledTracing = {
  enabled() {
    enabledCalls += 1;
    return false;
  },
  tracer() {
    tracerCalls += 1;
    throw new Error("disabled tracing resolved a tracer on the request path");
  }
};

const telemetryFree = makeHotPath(undefined);
const disabled = makeHotPath(disabledTracing);
const context = new MessageContext().withSampling(true);

function sample(stream) {
  const started = performance.now();
  for (let index = 0; index < iterations; index += 1) {
    stream.emit(context, index);
  }
  return performance.now() - started;
}

sample(telemetryFree);
sample(disabled);
const telemetryFreeSamples = [];
const disabledSamples = [];
for (let round = 0; round < rounds; round += 1) {
  if (round % 2 === 0) {
    telemetryFreeSamples.push(sample(telemetryFree));
    disabledSamples.push(sample(disabled));
  } else {
    disabledSamples.push(sample(disabled));
    telemetryFreeSamples.push(sample(telemetryFree));
  }
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)];
}

const telemetryFreeMs = median(telemetryFreeSamples);
const disabledMs = median(disabledSamples);
if (tracerCalls !== 0 || enabledCalls !== 1) {
  throw new Error(
    `disabled tracing boundary violated: enabled=${String(enabledCalls)} tracer=${String(tracerCalls)}`
  );
}
console.log(
  JSON.stringify(
    {
      iterations,
      rounds,
      telemetryFreeMs,
      disabledTracingMs: disabledMs,
      overheadPercent: ((disabledMs - telemetryFreeMs) / telemetryFreeMs) * 100,
      enabledCalls,
      tracerCalls
    },
    undefined,
    2
  )
);
