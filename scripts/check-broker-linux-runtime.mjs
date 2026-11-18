// Copy this probe into the unpacked SDK root and run there in the pinned Linux
// runtime. Dependencies may be reused, but every direct broker version is checked.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
assert.equal(process.platform, "linux");
assert.equal(process.version, `v${manifest.engines.node}`);
const entries = [];
for (const broker of ["nats", "pulsar", "redisstreams", "rabbitmq"]) {
  for (const direction of ["datasource", "datasink"]) {
    const specifier = `${manifest.name}/${direction}/${broker}`;
    const resolved = import.meta.resolve(specifier);
    assert.ok(fileURLToPath(resolved).startsWith(join(root, "dist") + "/"), resolved);
    assert.ok(Object.keys(await import(specifier)).length > 0);
    entries.push(specifier);
  }
}
const require = createRequire(import.meta.url);
const versions = {};
for (const name of [
  "@nats-io/transport-node",
  "@nats-io/jetstream",
  "pulsar-client",
  "redis",
  "rabbitmq-amqp-js-client"
]) {
  let location = dirname(require.resolve(name));
  let found;
  while (location !== dirname(location)) {
    try {
      const candidate = JSON.parse(await readFile(join(location, "package.json"), "utf8"));
      if (candidate.name === name) {
        found = candidate;
        break;
      }
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    location = dirname(location);
  }
  assert.ok(found, `missing installed manifest: ${name}`);
  assert.equal(found.version, manifest.dependencies[name], name);
  versions[name] = found.version;
}
// This exercises construction/destruction of the actual native addon without
// connecting to a broker. Module import alone is weaker than native execution.
const Pulsar = require("pulsar-client");
const client = new Pulsar.Client({
  serviceUrl: "pulsar://127.0.0.1:6650",
  operationTimeoutSeconds: 1
});
await client.close();
console.log(
  JSON.stringify(
    {
      status: "passed",
      sdk: manifest.version,
      node: process.version,
      platform: process.platform,
      architecture: process.arch,
      glibc: process.report.getReport().header.glibcVersionRuntime,
      entries,
      versions,
      nativePulsarConstructAndClose: true
    },
    null,
    2
  )
);
