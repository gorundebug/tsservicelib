import assert from "node:assert/strict";
import { access, readFile, readdir } from "node:fs/promises";
import { relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = new URL("../", import.meta.url);
const rootPath = fileURLToPath(root);
const packageJson = JSON.parse(await readFile(new URL("package.json", root), "utf8"));
const requiredExports = [
  ".",
  "./api",
  "./datasource",
  "./datasource/http",
  "./datasource/kafka",
  "./datasource/grpc",
  "./datasource/cron",
  "./datasource/temporal",
  "./datasource/temporal/workflow",
  "./datasource/localsource",
  "./datasink",
  "./datasink/http",
  "./datasink/kafka",
  "./datasink/grpc",
  "./datasink/localsink",
  "./datasink/temporal",
  "./operators",
  "./runtime",
  "./runtime/graph",
  "./transformation",
  "./runtime/config",
  "./runtime/config/workflow",
  "./runtime/logging",
  "./runtime/datastruct",
  "./runtime/pool",
  "./runtime/serde",
  "./runtime/status",
  "./runtime/store",
  "./runtime/telemetry",
  "./runtime/testmetrics",
  "./runtime/testlog",
  "./runtime/testtracing"
];

assert.equal(packageJson.type, "module", "the published package must be ESM");
assert.deepEqual(
  packageJson.files,
  ["dist", "README.md", "LICENSE"],
  "only distributable runtime files and public documentation may be packed"
);
assert.deepEqual(
  Object.keys(packageJson.exports),
  requiredExports,
  "public package exports must be explicit and complete"
);

for (const name of requiredExports) {
  assert.equal(name.includes("*"), false, `wildcard package export is forbidden: ${name}`);
  const entry = packageJson.exports[name];
  assert.deepEqual(
    Object.keys(entry),
    ["types", "import"],
    `package export ${name} must expose only types and ESM import targets`
  );
  assert.match(entry.types, /^\.\/dist\/.+\.d\.ts$/u);
  assert.match(entry.import, /^\.\/dist\/.+\.js$/u);

  const declaration = new URL(entry.types.slice(2), root);
  const implementation = new URL(entry.import.slice(2), root);
  await Promise.all([
    access(declaration),
    access(new URL(`${entry.types.slice(2)}.map`, root)),
    access(implementation),
    access(new URL(`${entry.import.slice(2)}.map`, root))
  ]);
  assert.match(
    await readFile(declaration, "utf8"),
    /\/\/# sourceMappingURL=.+\.d\.ts\.map\s*$/u,
    `declaration map is missing for ${name}`
  );
  assert.match(
    await readFile(implementation, "utf8"),
    /\/\/# sourceMappingURL=.+\.js\.map\s*$/u,
    `source map is missing for ${name}`
  );
}

async function javascriptFiles(path) {
  const files = [];
  for (const entry of await readdir(path, { withFileTypes: true })) {
    const child = resolve(path, entry.name);
    if (entry.isDirectory()) files.push(...(await javascriptFiles(child)));
    else if (entry.isFile() && entry.name.endsWith(".js")) files.push(child);
  }
  return files;
}

const copiedRuntimeAssets = new Set(["runtime/status/web/vis.min.js"]);
for (const implementation of await javascriptFiles(resolve(rootPath, "dist"))) {
  const output = relative(resolve(rootPath, "dist"), implementation);
  if (copiedRuntimeAssets.has(output)) continue;
  const source = resolve(rootPath, "src", output.replace(/\.js$/u, ".ts"));
  await assert.doesNotReject(
    access(source),
    `stale compiled output has no TypeScript source: dist/${output}`
  );
}
