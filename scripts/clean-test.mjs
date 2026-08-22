import { rm } from "node:fs/promises";

await Promise.all([
  rm(new URL("../dist-test", import.meta.url), { force: true, recursive: true }),
  rm(new URL("../.cache/tsconfig.test.tsbuildinfo", import.meta.url), { force: true })
]);
