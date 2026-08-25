import assert from "node:assert/strict";
import { test } from "node:test";

import {
  MessageContext,
  RuntimeConfig,
  applyDataSourceEndpointTracing,
  type EndpointConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironmentWithStore } from "./support/environment.js";

const endpoint = (tracingEnabled: boolean): EndpointConfig => ({
  id: 100,
  name: "input",
  idDataConnector: 10,
  properties: {},
  tracingEnabled
});

await test("DataSource endpoint tracing policy is read from each runtime config snapshot", () => {
  const { environment, store } = makeTestEnvironmentWithStore([], {
    dataConnectors: [
      {
        id: 10,
        name: "custom",
        type: 4,
        implementation: "custom",
        properties: {}
      }
    ],
    endpoints: [endpoint(false)]
  });
  const original = new MessageContext();
  assert.equal(applyDataSourceEndpointTracing(original, environment, 100).samplingEnabled(), false);

  const current = store.current().config();
  store.publish(new RuntimeConfig({ ...current, endpoints: [endpoint(true)] }));
  assert.equal(applyDataSourceEndpointTracing(original, environment, 100).samplingEnabled(), true);

  store.publish(new RuntimeConfig({ ...current, endpoints: [endpoint(false)] }));
  assert.equal(applyDataSourceEndpointTracing(original, environment, 100).samplingEnabled(), false);
  assert.equal(
    applyDataSourceEndpointTracing(original.withSampling(true), environment, 100).samplingEnabled(),
    true,
    "transport sampling remains active when the endpoint flag is disabled"
  );
});
