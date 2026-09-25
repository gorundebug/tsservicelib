import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";

import {
  Context,
  HashMapJoinStorage,
  MessageContext,
  StoreAlreadyStartedError,
  StoreStoppedError,
  type JoinStorageConfig,
  type JoinValues
} from "@gorundebug/tsservicelib/runtime";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
import { makeTestEnvironment } from "./support/environment.js";

class MutableConfig implements JoinStorageConfig {
  public ttl = 0;
  public renew = false;

  public ttlMs(): number {
    return this.ttl;
  }

  public renewTTL(): boolean {
    return this.renew;
  }

  public name(): string {
    return "test-join";
  }
}

function makeStorage(
  config = new MutableConfig(),
  metrics?: TestMetrics
): HashMapJoinStorage<string> {
  const environment = makeTestEnvironment([], metrics === undefined ? {} : { metrics });
  const storage = new HashMapJoinStorage<string>(environment, config);
  storage.start(Context.background());
  return storage;
}

function makeUnstartedStorage(
  config = new MutableConfig(),
  metrics?: TestMetrics
): HashMapJoinStorage<string> {
  const environment = makeTestEnvironment([], metrics === undefined ? {} : { metrics });
  return new HashMapJoinStorage<string>(environment, config);
}

function makeGate(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, resolve: release };
}

await test("join storage serializes callbacks per key and removes processed values", async () => {
  const storage = makeStorage();
  let active = 0;
  let maximumActive = 0;
  const snapshots: JoinValues[] = [];
  const callback = async (values: JoinValues): Promise<boolean> => {
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    await delay(2);
    snapshots.push(values.map((slot) => [...slot]));
    active -= 1;
    return values[0]?.length === 2;
  };

  await Promise.all([
    storage.joinValue(new MessageContext(), "order", 0, 1, callback),
    storage.joinValue(new MessageContext(), "order", 0, 2, callback)
  ]);

  assert.equal(maximumActive, 1);
  assert.deepEqual(snapshots, [[[1]], [[1, 2]]]);
  assert.equal(storage.size(), 0);
});

await test("join storage deadline invokes the last callback and evicts the key", async () => {
  const config = new MutableConfig();
  config.ttl = 1_000;
  const metrics = new TestMetrics();
  const storage = makeStorage(config, metrics);
  let expiredValues: JoinValues | undefined;
  let callbacks = 0;

  await storage.joinValue(new MessageContext().bounded(10), "deadline", 1, "right", (values) => {
    callbacks += 1;
    expiredValues = values.map((slot) => [...slot]);
    return false;
  });
  await delay(30);

  assert.deepEqual(expiredValues, [[], ["right"]]);
  assert.equal(callbacks, 2);
  assert.equal(storage.size(), 0);
  assert.equal(
    metrics.counterValue("hashmap_join_storage_evictions_total", {
      service: "test-service",
      name: "test-join"
    }),
    1
  );
});

await test("join storage cancellation with a deadline expires an item exactly once", async () => {
  const config = new MutableConfig();
  config.ttl = 60_000;
  const metrics = new TestMetrics();
  const storage = makeStorage(config, metrics);
  const cancellation = new AbortController();
  const context = new MessageContext()
    .bounded(60_000)
    .withExternalCancellation(cancellation.signal);
  let callbacks = 0;
  let reportExpired!: () => void;
  const expired = new Promise<void>((resolve) => {
    reportExpired = resolve;
  });

  await storage.joinValue(context, "cancelled", 0, "value", () => {
    callbacks += 1;
    if (callbacks === 2) reportExpired();
    return false;
  });
  cancellation.abort();
  await expired;
  await Promise.resolve();

  assert.equal(callbacks, 2);
  assert.equal(storage.size(), 0);
  assert.equal(
    metrics.counterValue("hashmap_join_storage_evictions_total", {
      service: "test-service",
      name: "test-join"
    }),
    1
  );
});

await test("join storage cancellation without a deadline preserves the configured TTL", async () => {
  const config = new MutableConfig();
  config.ttl = 60_000;
  const storage = makeStorage(config);
  const cancellation = new AbortController();
  const context = new MessageContext().withExternalCancellation(cancellation.signal);
  let callbacks = 0;
  try {
    await storage.joinValue(context, "cancelled", 0, "value", () => {
      callbacks += 1;
      return false;
    });
    cancellation.abort();
    await delay(0);
    assert.equal(callbacks, 1);
    assert.equal(storage.size(), 1);
  } finally {
    await storage.stop(Context.background());
  }
});

await test("join storage never republishes an expired group replaced during its callback", async (t) => {
  let now = Date.now();
  t.mock.method(Date, "now", () => now);
  const config = new MutableConfig();
  config.ttl = 60_000;
  config.renew = true;
  const storage = makeStorage(config);
  const started = makeGate();
  const released = makeGate();
  let pending: Promise<void> | undefined;
  try {
    await storage.joinValue(new MessageContext(), "key", 0, "old", () => false);
    pending = storage.joinValue(new MessageContext(), "key", 1, "held", async () => {
      started.resolve();
      await released.promise;
      return false;
    });
    await started.promise;
    now += 120_000;
    await storage.joinValue(new MessageContext(), "key", 0, "new", (values) => {
      assert.deepEqual(values, [["new"]]);
      return true;
    });
    assert.equal(storage.size(), 0);
    released.resolve();
    await pending;
    assert.equal(storage.size(), 0);
  } finally {
    released.resolve();
    await pending;
    await storage.stop(Context.background());
  }
});

await test("join storage queued expiration respects renewal by the active callback", async () => {
  const config = new MutableConfig();
  config.ttl = 30;
  config.renew = true;
  const storage = makeStorage(config);
  const started = makeGate();
  const released = makeGate();
  let callbacks = 0;
  let pending: Promise<void> | undefined;
  try {
    await storage.joinValue(new MessageContext(), "key", 0, "first", () => {
      callbacks += 1;
      return false;
    });
    config.ttl = 60_000;
    pending = storage.joinValue(new MessageContext(), "key", 1, "second", async () => {
      callbacks += 1;
      started.resolve();
      await released.promise;
      return false;
    });
    await started.promise;
    await delay(60);
    released.resolve();
    await pending;
    await delay(0);
    assert.equal(callbacks, 2);
    assert.equal(storage.size(), 1);
  } finally {
    released.resolve();
    await pending;
    await storage.stop(Context.background());
  }
});

await test("join storage reads renew TTL dynamically without retaining config values", async () => {
  const config = new MutableConfig();
  config.ttl = 30;
  config.renew = true;
  const storage = makeStorage(config);
  let expirations = 0;
  const callback = (): boolean => {
    expirations += 1;
    return false;
  };

  await storage.joinValue(new MessageContext(), "renew", 0, 1, callback);
  await delay(20);
  config.ttl = 50;
  await storage.joinValue(new MessageContext(), "renew", 0, 2, callback);
  const callsAfterValues = expirations;
  await delay(25);
  assert.equal(expirations, callsAfterValues);
  await delay(40);
  assert.equal(expirations, callsAfterValues + 1);
  assert.equal(storage.size(), 0);
});

await test("join storage lifecycle rejects duplicate start and start after stop", async () => {
  const storage = makeUnstartedStorage();
  await storage.joinValue(new MessageContext(), "before-start", 0, 1, () => true);
  storage.start(Context.background());
  assert.throws(() => {
    storage.start(Context.background());
  }, StoreAlreadyStartedError);
  await storage.stop(Context.background());
  await storage.stop(Context.background());
  await storage.joinValue(new MessageContext(), "after-stop", 0, 1, () => true);
  assert.throws(() => {
    storage.start(Context.background());
  }, StoreStoppedError);
});

await test("join storage stop waits for active TTL joins without clearing their groups", async () => {
  const config = new MutableConfig();
  config.ttl = 60_000;
  const storage = makeStorage(config);
  let releaseCallback!: () => void;
  const released = new Promise<void>((resolve) => {
    releaseCallback = resolve;
  });
  let reportStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    reportStarted = resolve;
  });

  const admitted = storage.joinValue(new MessageContext(), "key", 0, 1, async () => {
    reportStarted();
    await released;
    return false;
  });
  await started;

  let stopped = false;
  const stopping = storage.stop(Context.background()).then(() => {
    stopped = true;
  });
  await Promise.resolve();
  assert.equal(stopped, false);
  releaseCallback();
  await Promise.all([admitted, stopping]);

  assert.equal(stopped, true);
  assert.equal(storage.size(), 1);
  await storage.joinValue(new MessageContext(), "key", 1, 2, (values) => {
    assert.deepEqual(values, [[1], [2]]);
    return true;
  });
  assert.equal(storage.size(), 0);
});

await test("join storage registers and updates the exact canonical metrics", async () => {
  const metrics = new TestMetrics();
  const storage = makeStorage(new MutableConfig(), metrics);
  const labels = { service: "test-service", name: "test-join" };
  assert.deepEqual(
    metrics.registeredNames().filter((name) => name.startsWith("hashmap_join_storage_")),
    ["hashmap_join_storage_count", "hashmap_join_storage_evictions_total"]
  );
  assert.equal(metrics.gaugeValue("hashmap_join_storage_count", labels), 0);
  await storage.joinValue(new MessageContext(), "pending", 0, 1, () => false);
  assert.equal(metrics.gaugeValue("hashmap_join_storage_count", labels), 1);
  await storage.joinValue(new MessageContext(), "pending", 1, 2, () => true);
  assert.equal(metrics.gaugeValue("hashmap_join_storage_count", labels), 0);
  assert.equal(metrics.counterValue("hashmap_join_storage_evictions_total", labels), 0);
});
