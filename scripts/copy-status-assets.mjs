import { cp, mkdir } from "node:fs/promises";

const output = process.argv[2];
if (output === undefined || output.length === 0) {
  throw new Error("usage: node scripts/copy-status-assets.mjs <output-directory>");
}

const source = new URL("../resources/status/", import.meta.url);
const destination = new URL(
  `../${output.replace(/^\/+|\/+$/gu, "")}/runtime/status/web/`,
  import.meta.url
);
await mkdir(destination, { recursive: true });
await cp(source, destination, { recursive: true });
