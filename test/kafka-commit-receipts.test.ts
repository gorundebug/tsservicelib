import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

await test("Kafka native receipt matching preserves partition and assignment boundaries", () => {
  // Use the built package and controlled native callbacks, without network/brokers.
  execFileSync(
    process.execPath,
    [
      "--test",
      fileURLToPath(new URL("../../test/native-probes/kafka-commit-receipts.mjs", import.meta.url))
    ],
    { timeout: 30_000, stdio: "pipe" }
  );
});
