import assert from "node:assert/strict";
import { test } from "node:test";

import { parseConfigArguments } from "@gorundebug/tsservicelib/runtime/config";

await test("config arguments have the canonical example defaults", () => {
  assert.deepEqual(parseConfigArguments([]), {
    configPath: "./config/config.yaml",
    valuesPath: "./config/overrides.yaml"
  });
});

await test("config arguments support Go-compatible flag and equals forms", () => {
  assert.deepEqual(
    parseConfigArguments(["-config", "/run/base.yaml", "--values=/run/values.yaml"]),
    {
      configPath: "/run/base.yaml",
      valuesPath: "/run/values.yaml"
    }
  );
});

await test("config arguments reject missing values and unknown flags", () => {
  assert.throws(() => parseConfigArguments(["--config"]), /missing value/);
  assert.throws(() => parseConfigArguments(["--unknown"]), /unknown command-line argument/);
});
