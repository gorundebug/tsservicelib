import assert from "node:assert/strict";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { parse as parseYaml } from "yaml";

const root = resolve(new URL("..", import.meta.url).pathname);
const temporary = await mkdtemp(join(tmpdir(), "tsservicelib-package-"));
const sourcePackage = JSON.parse(await readFile(join(root, "package.json"), "utf8"));

try {
  run("corepack", ["pnpm", "pack", "--pack-destination", temporary], root);
  const tarball = (await readdir(temporary)).find((name) => name.endsWith(".tgz"));
  assert.notEqual(tarball, undefined, "pnpm pack did not create a package archive");
  const archive = join(temporary, tarball);
  const files = run("tar", ["-tf", archive], root).split("\n").filter(Boolean);
  assert.ok(files.includes("package/package.json"), "packed package.json is missing");
  assert.ok(
    files.some((name) => name.startsWith("package/dist/")),
    "packed dist is missing"
  );
  for (const name of files) {
    assert.equal(
      name.startsWith("package/src/"),
      false,
      `source file leaked into package: ${name}`
    );
    assert.equal(name.startsWith("package/test/"), false, `test file leaked into package: ${name}`);
    assert.equal(
      name.startsWith("package/scripts/"),
      false,
      `build script leaked into package: ${name}`
    );
  }

  await writeFile(
    join(temporary, "package.json"),
    `${JSON.stringify(
      {
        private: true,
        type: "module",
        dependencies: { "@gorundebug/tsservicelib": `file:${archive}` },
        devDependencies: {
          "@types/node": sourcePackage.devDependencies["@types/node"],
          typescript: sourcePackage.devDependencies.typescript
        }
      },
      null,
      2
    )}\n`
  );
  // Consumer installation uses the supported native-build and dependency pins.
  // In particular the official RabbitMQ client otherwise resolves rhea via SSH.
  await writeFile(
    join(temporary, "pnpm-workspace.yaml"),
    await readFile(join(root, "pnpm-workspace.yaml"), "utf8")
  );
  // Retain the reviewed transitive resolutions when adding the packed SDK.
  // Without this, a clean consumer resolves newer semver-compatible dependencies.
  await writeFile(
    join(temporary, "pnpm-lock.yaml"),
    await readFile(join(root, "pnpm-lock.yaml"), "utf8")
  );
  const store = run("corepack", ["pnpm", "store", "path"], root).trim();
  run("corepack", ["pnpm", "install", "--offline", "--no-frozen-lockfile", "--store-dir", store, "--dir", temporary], root);

  const referenceLock = parseYaml(await readFile(join(root, "pnpm-lock.yaml"), "utf8"));
  const consumerLock = parseYaml(await readFile(join(temporary, "pnpm-lock.yaml"), "utf8"));
  for (const identity of Object.keys(consumerLock.packages ?? {})) {
    if (identity.startsWith(`${sourcePackage.name}@file:`) && identity.endsWith(`/${tarball}`)) continue;
    assert.ok(Object.hasOwn(referenceLock.packages, identity),
      `Packed consumer resolved an unreviewed dependency: ${identity}`);
  }

  await writeFile(
    join(temporary, "consumer.mjs"),
    [
      'import assert from "node:assert/strict";',
      'import * as root from "@gorundebug/tsservicelib";',
      'import * as runtime from "@gorundebug/tsservicelib/runtime";',
      'import * as operators from "@gorundebug/tsservicelib/operators";',
      'assert.equal(typeof root, "object");',
      'assert.equal(typeof runtime.Context.background, "function");',
      "assert.ok(Object.keys(operators).length > 0);",
      ...["nats", "pulsar", "redisstreams", "rabbitmq"].flatMap((broker) =>
        ["datasource", "datasink"].map((direction) =>
          `assert.ok(Object.keys(await import("@gorundebug/tsservicelib/${direction}/${broker}")).length > 0);`
        )
      )
    ].join("\n")
  );
  run(process.execPath, [join(temporary, "consumer.mjs")], temporary);

  await writeFile(
    join(temporary, "consumer.ts"),
    [
      'import { Context } from "@gorundebug/tsservicelib/runtime";',
      'import type { RuntimeConfig } from "@gorundebug/tsservicelib/runtime";',
      "const context: Context = Context.background();",
      "const config: RuntimeConfig | undefined = undefined;",
      "void context;",
      "void config;",
      ...["nats", "pulsar", "redisstreams", "rabbitmq"].flatMap((broker) =>
        ["datasource", "datasink"].flatMap((direction) => [
          `import * as ${direction}_${broker} from "@gorundebug/tsservicelib/${direction}/${broker}";`,
          `void ${direction}_${broker};`
        ])
      )
    ].join("\n")
  );
  await writeFile(
    join(temporary, "tsconfig.json"),
    `${JSON.stringify(
      {
        compilerOptions: {
          module: "NodeNext",
          moduleResolution: "NodeNext",
          strict: true,
          noEmit: true,
          skipLibCheck: false,
          types: ["node"]
        },
        files: ["consumer.ts"]
      },
      null,
      2
    )}\n`
  );
  run(
    process.execPath,
    [
      join(root, "node_modules", "typescript", "bin", "tsc"),
      "--project",
      join(temporary, "tsconfig.json")
    ],
    temporary
  );

  const installedPackage = JSON.parse(
    await readFile(
      join(temporary, "node_modules", "@gorundebug", "tsservicelib", "package.json"),
      "utf8"
    )
  );
  assert.equal(installedPackage.type, "module");
  assert.equal(installedPackage.version, sourcePackage.version);
  console.log(`PASS packed SDK ${installedPackage.version}: root/runtime, eight broker imports and TypeScript declarations; Node ${process.version}`);
} finally {
  await rm(temporary, { recursive: true, force: true });
}

function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8" });
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} failed (${String(result.status)}):\n${result.stdout}${result.stderr}`
    );
  }
  return result.stdout;
}
