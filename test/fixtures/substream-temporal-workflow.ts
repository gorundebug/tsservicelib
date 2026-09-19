import { sleep } from "@temporalio/workflow";

import { TemporalWorkflowEnvironment } from "@gorundebug/tsservicelib/datasource/temporal/workflow";
import { makeMapStream, makeSubStream } from "@gorundebug/tsservicelib/operators";
import {
  MessageContext,
  SubStreamCollectorFunc,
  makeDefaultSerdeRegistry,
  type CanonicalConfig,
  type MapStreamConfig,
  type SubStreamConfig
} from "@gorundebug/tsservicelib/runtime/graph";

function requireCondition(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

export async function substreamReplayWorkflow(count: number): Promise<number[]> {
  const common = {
    properties: {},
    pipeline: "substream-replay",
    idService: 1,
    idSources: [],
    xPos: 0,
    yPos: 0,
    valueType: "int32"
  };
  const entryConfig: SubStreamConfig = {
    ...common,
    id: 1,
    name: "Lookup",
    type: "SubStream",
    idSource: 2
  };
  const bodyConfig: MapStreamConfig = {
    ...common,
    id: 2,
    name: "Normalize",
    type: "Map",
    idSource: 1
  };
  const config: CanonicalConfig = {
    services: [
      {
        id: 1,
        name: "workflow-service",
        color: "#000000",
        environment: "test",
        grpcHost: "",
        grpcPort: 0,
        httpHost: "",
        httpPort: 0,
        metricsHandler: "/metrics",
        shutdownTimeout: 1_000,
        statusHandler: "/status",
        startupHandler: "/health/startup",
        readinessHandler: "/health/ready",
        livenessHandler: "/health/live",
        kubernetesWorkloadType: "Deployment",
        properties: {}
      }
    ],
    streams: [entryConfig, bodyConfig],
    dataConnectors: [],
    endpoints: [],
    pools: [],
    links: [],
    modules: [],
    types: [],
    properties: {}
  };
  const environment = new TemporalWorkflowEnvironment(config, 1, makeDefaultSerdeRegistry());
  const entry = makeSubStream<number, number>(entryConfig, environment);
  const body = makeMapStream<number, number>(bodyConfig, entry, {
    async map(context, _stream, value, out): Promise<void> {
      if (value % 2 === 1) {
        await entry.consume(
          context,
          value - 1,
          new SubStreamCollectorFunc(async (caller, result) => {
            requireCondition(caller === context, "Nested caller context changed");
            await out.out(caller, result + 10);
            return true;
          })
        );
      } else {
        await sleep(10 + (value % 3));
        await out.out(context, value * 10);
      }
      await out.out(context, -1);
    }
  });
  entry.setSource(body);
  await environment.start();
  const parent = new MessageContext().withStreamId("shared-workflow-parent");

  async function invoke(value: number): Promise<number> {
    const results: number[] = [];
    await entry.consume(
      parent,
      value,
      new SubStreamCollectorFunc(async (caller, result) => {
        requireCondition(caller === parent, "Caller context was not restored");
        results.push(result);
        await sleep(1);
        return true;
      })
    );
    requireCondition(
      results.length === 1 && results[0] === value * 10,
      `SubStream results crossed or arrived late for ${String(value)}`
    );
    return value * 10;
  }

  try {
    return await Promise.all(Array.from({ length: count }, (_, value) => invoke(value)));
  } finally {
    await environment.finish();
  }
}
