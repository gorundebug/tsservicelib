import { rm } from "node:fs/promises";

await Promise.all(
  ["dist", "dist-test", ".cache", "coverage"].map(async (path) => {
    await rm(new URL(`../${path}`, import.meta.url), { force: true, recursive: true });
  })
);
