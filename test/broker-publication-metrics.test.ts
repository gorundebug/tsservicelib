import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

await test("broker publication counters preserve adapter completion boundaries", () => {
  // Use the built package and mocked native sessions, without network/brokers.
  execFileSync(
    process.execPath,
    [
      "--test",
      fileURLToPath(
        new URL("../../test/native-probes/broker-publication-metrics.mjs", import.meta.url)
      )
    ],
    { timeout: 30_000, stdio: "pipe" }
  );
});
