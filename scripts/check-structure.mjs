import { access, readdir } from "node:fs/promises";

const requiredRoots = ["api", "datasink", "datasource", "operators", "runtime", "transformation"];
const requiredRuntimePackages = [
  "config",
  "datastruct",
  "environment",
  "logging",
  "pool",
  "serde",
  "status",
  "store",
  "testmetrics",
  "testlog",
  "testtracing",
  "telemetry"
];

for (const path of requiredRoots) {
  await access(new URL(`../src/${path}/`, import.meta.url));
}
for (const path of requiredRuntimePackages) {
  await access(new URL(`../src/runtime/${path}/`, import.meta.url));
}

const sourceEntries = await readdir(new URL("../src/", import.meta.url));
const misplaced = requiredRuntimePackages.filter((path) => sourceEntries.includes(path));
if (misplaced.length > 0) {
  throw new Error(
    `runtime-owned packages must be nested under src/runtime: ${misplaced.join(", ")}`
  );
}
