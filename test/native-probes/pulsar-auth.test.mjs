import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import Pulsar from "pulsar-client";
import { Session } from "../../dist/internal/pulsar/client.js";
import { Context, MessageContext } from "../../dist/runtime/index.js";
import { startFixture } from "./pulsar-auth-fixture.mjs";
const enabled = { skip: process.env.PULSAR_AUTH_TEST !== "1", timeout: 20000 };
let fixture;
const sessions = new Set();
const context = () => new Context().bounded(3000);
async function admin(method, path, body, role = "admin") {
  const r = await fetch(fixture.adminUrl + path, {
    method,
    headers: {
      authorization: "Bearer " + fixture.tokens[role],
      "content-type": "application/json"
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(3000)
  });
  const text = await r.text();
  return { status: r.status, body: text ? JSON.parse(text) : undefined };
}
function session(role, source, topic = "allowed", namespace = "events", adminRole) {
  const s = new Session(
    {
      id: 11,
      name: "auth",
      type: 11,
      implementation: "pulsar-client-node",
      properties: {},
      servers: [fixture.url],
      adminUrl: fixture.adminUrl,
      token: fixture.tokens[role],
      adminToken: adminRole ? fixture.tokens[adminRole] : undefined,
      connectTimeoutMs: 1000,
      operationTimeoutMs: 2000
    },
    {
      id: source ? 1 : 2,
      name: "endpoint",
      idDataConnector: 11,
      enabled: true,
      properties: {},
      topic: `persistent://acl/${namespace}/${topic}`,
      ...(source ? { subscriptionName: "workers" } : {})
    },
    source,
    () => {}
  );
  sessions.add(s);
  return s;
}
before(
  async () => {
    if (enabled.skip) return;
    fixture = await startFixture();
    assert.equal(
      (
        await admin("PUT", "/admin/v2/tenants/acl", {
          adminRoles: ["admin", "tenant-admin"],
          allowedClusters: ["standalone"]
        })
      ).status,
      204
    );
    for (const ns of ["events", "denied"]) {
      assert.equal((await admin("PUT", `/admin/v2/namespaces/acl/${ns}`, {})).status, 204);
      for (const [policy, field] of [
        ["autoTopicCreation", "allowAutoTopicCreation"],
        ["autoSubscriptionCreation", "allowAutoSubscriptionCreation"]
      ])
        assert.equal(
          (await admin("POST", `/admin/v2/namespaces/acl/${ns}/${policy}`, { [field]: false }))
            .status,
          204
        );
      for (const topic of ["allowed", "forbidden"]) {
        assert.equal((await admin("PUT", `/admin/v2/persistent/acl/${ns}/${topic}`)).status, 204);
        assert.equal(
          (
            await admin("PUT", `/admin/v2/persistent/acl/${ns}/${topic}/subscription/workers`, {
              ledgerId: -1,
              entryId: -1
            })
          ).status,
          204
        );
      }
    }
    for (const [role, actions] of [
      ["producer", ["produce"]],
      ["consumer", ["consume"]]
    ])
      assert.equal(
        (await admin("POST", `/admin/v2/namespaces/acl/events/permissions/${role}`, actions))
          .status,
        204
      );
    for (const [role, actions] of [
      ["topic-producer", ["produce"]],
      ["topic-consumer", ["consume"]]
    ])
      assert.equal(
        (
          await admin(
            "POST",
            `/admin/v2/persistent/acl/events/allowed/permissions/${role}`,
            actions
          )
        ).status,
        204
      );
  },
  { timeout: 130000 }
);
after(async () => {
  for (const s of sessions) await s.close(context()).catch(() => {});
  for (const c of clients) await closeClient(c);
  await fixture?.close();
});
test("authenticated SDK admin-capable token roundtrip and bounded close", enabled, async () => {
  const input = session("admin", true),
    output = session("admin", false);
  await input.start(context());
  await output.start(context());
  const result = await output.publish(
    { data: Buffer.from("authenticated") },
    new MessageContext().bounded(3000)
  );
  assert.equal(result.outcome, "Accepted");
  const m = await input.receive(context());
  assert.equal(m.getData().toString(), "authenticated");
  await input.settle(m, input.epoch, context());
  await input.close(context());
  await output.close(context());
});
const logs = [],
  errors = [],
  clients = new Set();
function safe(error) {
  errors.push(String(error));
  assert.equal(
    Object.values(fixture.tokens).some((token) => String(error).includes(token)),
    false,
    "error must not expose token"
  );
  return true;
}
function client(role, token) {
  const c = new Pulsar.Client({
    serviceUrl: fixture.url,
    authentication: new Pulsar.AuthenticationToken({ token: token ?? fixture.tokens[role] }),
    operationTimeoutSeconds: 2,
    connectionTimeoutMs: 500,
    log: (level, file, line, message) => {
      logs.push(message);
    }
  });
  clients.add(c);
  return c;
}
async function closeClient(c) {
  const started = performance.now();
  await Promise.race([
    c.close(),
    new Promise((_, reject) => {
      const timer = setTimeout(() => reject(Error("native close exceeded 3s")), 3000);
      timer.unref();
    })
  ]);
  clients.delete(c);
  assert.ok(performance.now() - started < 3000);
}
for (const role of ["producer", "consumer", "topic-producer", "topic-consumer", "outsider"])
  test(`SDK fails closed for ${role} without namespace-policy admin rights`, enabled, async () => {
    const s = session(role, role.includes("consumer"));
    await assert.rejects(s.start(context()), (error) => {
      assert.equal(error.status, 403);
      return safe(error);
    });
    const began = performance.now();
    await s.close(context());
    assert.ok(performance.now() - began < 1000);
  });
for (const topicOnly of [false, true])
  test(
    `official data-plane token roundtrip ${topicOnly ? "topic" : "namespace"} ACL`,
    enabled,
    async () => {
      const producer = client(topicOnly ? "topic-producer" : "producer"),
        consumer = client(topicOnly ? "topic-consumer" : "consumer");
      try {
        const input = await consumer.subscribe({
          topic: "persistent://acl/events/allowed",
          subscription: "workers",
          subscriptionType: "Shared",
          receiverQueueSize: 1
        });
        const output = await producer.createProducer({
          topic: "persistent://acl/events/allowed",
          batchingEnabled: false,
          sendTimeoutMs: 1500,
          blockIfQueueFull: false,
          maxPendingMessages: 1
        });
        const id = await output.send({ data: Buffer.from("scoped") });
        const message = await input.receive();
        assert.deepEqual(message.getMessageId().serialize(), id.serialize());
        await input.acknowledge(message);
        const pending = input.receive();
        const settled = assert.rejects(pending, safe);
        await input.close();
        await settled;
      } finally {
        await closeClient(producer);
        await closeClient(consumer);
      }
    }
  );
for (const [role, source, topic, ns] of [
  ["producer", true, "allowed", "events"],
  ["consumer", false, "allowed", "events"],
  ["producer", false, "allowed", "denied"],
  ["consumer", true, "allowed", "denied"],
  ["topic-producer", false, "forbidden", "events"],
  ["topic-consumer", true, "forbidden", "events"],
  ["outsider", false, "allowed", "events"]
])
  test(
    `official ACL denies ${role} ${source ? "consume" : "produce"} ${ns}/${topic}`,
    enabled,
    async () => {
      const c = client(role),
        address = `persistent://acl/${ns}/${topic}`;
      try {
        await assert.rejects(
          source
            ? c.subscribe({ topic: address, subscription: "workers", subscriptionType: "Shared" })
            : c.createProducer({ topic: address, batchingEnabled: false }),
          (error) => {
            assert.match(error.message, /AuthorizationError/);
            return safe(error);
          }
        );
      } finally {
        await closeClient(c);
      }
    }
  );
test(
  "invalid and missing tokens fail admin authentication without exposing credentials",
  enabled,
  async () => {
    for (const token of ["", fixture.tokens.admin.slice(0, -5) + "xxxxx"]) {
      const r = await fetch(
        fixture.adminUrl + "/admin/v2/namespaces/acl/events/autoTopicCreation",
        {
          headers: token ? { authorization: "Bearer " + token } : {},
          signal: AbortSignal.timeout(2000)
        }
      );
      assert.equal(r.status, 401);
      safe(await r.text());
      const s = session("admin", false);
      s.config.token = token;
      await assert.rejects(s.start(context()), (error) => {
        assert.equal(error.status, 401);
        assert.equal(String(error).includes(token) && !!token, false);
        return safe(error);
      });
      await s.close(context());
    }
  }
);
test("invalid token fails native authentication and close stays bounded", enabled, async () => {
  const invalid = fixture.tokens.admin.slice(0, -5) + "xxxxx",
    c = client("admin", invalid);
  try {
    await assert.rejects(
      c.createProducer({ topic: "persistent://acl/events/allowed" }),
      (error) => {
        assert.match(error.message, /AuthenticationError|ConnectError|TimeOut/);
        assert.equal(String(error).includes(invalid), false);
        return safe(error);
      }
    );
  } finally {
    await closeClient(c);
  }
});
test(
  "captured official-client diagnostics and exposed errors contain no authentication values",
  enabled,
  () => {
    const all = logs.concat(errors).join("\n");
    assert.equal(
      Object.values(fixture.tokens).some((token) => all.includes(token)),
      false
    );
    assert.ok(errors.length > 0);
    assert.equal(
      fixture.secretFreeBrokerLogs(),
      true,
      "broker logs must not expose test credentials"
    );
  }
);

test("SDK tenant-admin preflight plus scoped native tokens", enabled, async () => {
  for (const [producer, consumer] of [
    ["producer", "consumer"],
    ["topic-producer", "topic-consumer"]
  ]) {
    const input = session(consumer, true, "allowed", "events", "tenant-admin"),
      output = session(producer, false, "allowed", "events", "tenant-admin");
    await input.start(context());
    await output.start(context());
    const result = await output.publish(
      { data: Buffer.from("separate") },
      new MessageContext().bounded(3000)
    );
    assert.equal(result.outcome, "Accepted");
    const m = await input.receive(context());
    assert.deepEqual(m.getMessageId().serialize(), result.id.serialize());
    await input.settle(m, input.epoch, context());
    await input.close(context());
    await output.close(context());
  }
});
for (const [role, source, topic, ns] of [
  ["producer", true, "allowed", "events"],
  ["consumer", false, "allowed", "events"],
  ["topic-producer", false, "forbidden", "events"],
  ["producer", false, "allowed", "denied"]
])
  test(
    `SDK native ACL remains scoped despite tenant admin ${role}/${topic}/${ns}`,
    enabled,
    async () => {
      const s = session(role, source, topic, ns, "tenant-admin");
      await assert.rejects(s.start(context()), (error) => {
        assert.match(error.message, /AuthorizationError/);
        return safe(error);
      });
      await s.close(context());
    }
  );
test(
  "SDK explicit admin rejection never falls back to valid native admin token",
  enabled,
  async () => {
    const s = session("admin", false, "allowed", "events", "outsider");
    await assert.rejects(s.start(context()), (e) => {
      assert.equal(e.status, 403);
      return safe(e);
    });
    await s.close(context());
    const bad = session("admin", false);
    bad.config.adminToken = "invalid-admin-token";
    await assert.rejects(bad.start(context()), (e) => {
      assert.equal(e.status, 401);
      return safe(e);
    });
    await bad.close(context());
  }
);

for (const language of ["go", "python"])
  test(
    `isolated ${language} SDK adminToken acceptance`,
    { skip: enabled.skip || process.env.PULSAR_AUTH_CROSS_LANGUAGE !== "1", timeout: 180000 },
    async () => {
      const { execFile } = await import("node:child_process"),
        { promisify } = await import("node:util");
      const { fileURLToPath } = await import("node:url");
      const root = fileURLToPath(new URL("../../../", import.meta.url));
      const command =
        language === "go"
          ? (process.env.PULSAR_TEST_GO ?? "/Users/sergeyalexeev/sdk/go1.26.4/bin/go")
          : root + "pyservicelib/.venv/bin/python";
      const args =
        language === "go"
          ? [
              "test",
              "-race",
              "./internal/pulsarclient",
              "./runtime/config",
              "-run",
              "TestNativePulsarAdminTokenSeparation|TestPulsar",
              "-count=1"
            ]
          : [
              "-m",
              "pytest",
              "-q",
              "tests/test_pulsar_admin_token_native.py",
              "tests/test_pulsar_config.py"
            ];
      try {
        const result = await promisify(execFile)(command, args, {
          cwd: root + (language === "go" ? "servicelib" : "pyservicelib"),
          env: { ...process.env, PULSAR_AUTH_MANIFEST: fixture.manifestPath },
          timeout: 170000,
          maxBuffer: 4 * 1024 * 1024
        });
        safe(result.stdout + result.stderr);
      } catch (error) {
        const output = String(error.stdout ?? "") + String(error.stderr ?? "");
        assert.equal(
          Object.values(fixture.tokens).some((token) => output.includes(token)),
          false,
          "child diagnostics expose a credential"
        );
        throw new Error(language + " acceptance failed: " + output);
      }
    }
  );
test("separated credentials stay out of native and broker diagnostics", enabled, () => {
  safe(logs.join("\n"));
  assert.equal(fixture.secretFreeBrokerLogs(), true);
});

test("tenant admin can provision explicitly without broker-superuser access", enabled, async () => {
  assert.equal(
    (await admin("GET", "/admin/v2/brokers/standalone", undefined, "tenant-admin")).status,
    401
  );
  const output = session("producer", false, "explicit-created", "events", "tenant-admin");
  output.ep.createTopic = true;
  await output.start(context());
  const input = session("consumer", true, "explicit-created", "events", "tenant-admin");
  input.ep.createSubscription = true;
  await input.start(context());
  await input.close(context());
  await output.close(context());
  assert.equal(fixture.secretFreeBrokerLogs(), true);
});
