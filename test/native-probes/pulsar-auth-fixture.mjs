/** Isolated official broker. Tokens stay in owner-only files, never Docker arguments. */
import { randomBytes, randomUUID, createHmac } from "node:crypto";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync, spawnSync } from "node:child_process";
import net from "node:net";
import { once } from "node:events";
import { pathToFileURL } from "node:url";
async function port() {
  const s = net.createServer();
  s.listen(0, "127.0.0.1");
  await once(s, "listening");
  const p = s.address().port;
  await new Promise((r) => s.close(r));
  return p;
}
export async function startFixture() {
  const dir = await mkdtemp(join(tmpdir(), "ts-pulsar-auth-")),
    name = "ts-pulsar-auth-" + randomUUID(),
    secret = randomBytes(32),
    binary = await port(),
    web = await port();
  const token = (role) => {
    const parts = [
      { alg: "HS256", typ: "JWT" },
      { sub: role, exp: Math.floor(Date.now() / 1000) + 3600 }
    ]
      .map((v) => Buffer.from(JSON.stringify(v)).toString("base64url"))
      .join(".");
    return parts + "." + createHmac("sha256", secret).update(parts).digest("base64url");
  };
  const tokens = Object.fromEntries(
    [
      "admin",
      "tenant-admin",
      "producer",
      "consumer",
      "outsider",
      "topic-producer",
      "topic-consumer"
    ].map((r) => [r, token(r)])
  );
  const image = process.env.PULSAR_AUTH_IMAGE ?? "localhost:18083/apachepulsar/pulsar:4.1.2";
  const manifest = {
    name,
    image,
    url: `pulsar://127.0.0.1:${binary}`,
    adminUrl: `http://127.0.0.1:${web}`,
    tokens
  };
  await writeFile(join(dir, "secret.key"), secret, { mode: 0o600 });
  await writeFile(join(dir, "admin.token"), tokens.admin, { mode: 0o600 });
  await writeFile(
    join(dir, "extra.conf"),
    `\nauthenticationEnabled=true\nauthorizationEnabled=true\nauthenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken\ntokenSecretKey=file:///fixture/secret.key\nsuperUserRoles=admin\nbrokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken\nbrokerClientAuthenticationParameters=file:///fixture/admin.token\nadvertisedAddress=127.0.0.1\nbrokerServicePort=${binary}\nwebServicePort=${web}\nautoSkipNonRecoverableData=false\n`,
    { mode: 0o600 }
  );
  const manifestPath = join(dir, "manifest.json");
  await writeFile(manifestPath, JSON.stringify(manifest), { mode: 0o600 });
  let started = false;
  const close = async () => {
    if (started) {
      execFileSync("docker", ["rm", "-fv", name], { stdio: "ignore" });
      started = false;
    }
    await rm(dir, { recursive: true, force: true });
  };
  try {
    execFileSync(
      "docker",
      [
        "run",
        "-d",
        "--name",
        name,
        "--label",
        "sa.test=pulsar-token-acl",
        "--user",
        "0",
        "-p",
        `127.0.0.1:${binary}:${binary}`,
        "-p",
        `127.0.0.1:${web}:${web}`,
        "-v",
        `${dir}:/fixture:ro`,
        "-e",
        "PULSAR_MEM=-Xms256m -Xmx512m -XX:MaxDirectMemorySize=256m",
        image,
        "bash",
        "-c",
        "cat /fixture/extra.conf >> conf/standalone.conf; exec bin/pulsar standalone --no-functions-worker --no-stream-storage"
      ],
      { stdio: "pipe" }
    );
    started = true;
    const deadline = Date.now() + 120000;
    while (true) {
      try {
        const r = await fetch(manifest.adminUrl + "/admin/v2/clusters", {
          headers: { authorization: "Bearer " + tokens.admin },
          signal: AbortSignal.timeout(1000)
        });
        if (r.ok) {
          const clusters = await r.json();
          if (clusters.includes("standalone")) break;
        } else await r.arrayBuffer();
      } catch {}
      if (Date.now() > deadline) throw Error("Isolated authenticated Pulsar startup timed out");
      await new Promise((r) => setTimeout(r, 500));
    }
    return {
      ...manifest,
      manifestPath,
      close,
      secretFreeBrokerLogs: () => {
        const result = spawnSync("docker", ["logs", name], {
          encoding: "utf8",
          maxBuffer: 16 * 1024 * 1024
        });
        if (result.status !== 0) throw Error("Cannot inspect fixture broker logs");
        const log = result.stdout + result.stderr;
        return ![...Object.values(tokens), secret.toString("base64")].some((value) =>
          log.includes(value)
        );
      }
    };
  } catch (error) {
    await close();
    throw new Error("Authenticated Pulsar fixture failed to start", {
      cause: new Error(error.name)
    });
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const fixture = await startFixture();
  console.log(fixture.manifestPath);
  for (const signal of ["SIGINT", "SIGTERM"])
    process.once(signal, () => {
      void fixture.close().then(() => process.exit(0));
    });
  setInterval(() => {}, 1000);
}
