import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";

import { Context, HashMapJoinStorage, MessageContext } from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

function gate(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, resolve: release };
}

async function bounded<T>(operation: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error("join storage operation did not complete"));
        }, 1000);
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function storage(ttl: number): HashMapJoinStorage<string> {
  const result = new HashMapJoinStorage<string>(makeTestEnvironment([]), {
    ttlMs: () => ttl,
    renewTTL: () => false,
    name: () => "stop-parity"
  });
  result.start(Context.background());
  return result;
}

await test("join storage stop preserves an already accepted expiry callback", async () => {
  const subject = storage(10);
  const expired = gate();
  let calls = 0;
  await subject.joinValue(new MessageContext(), "key", 0, "value", () => {
    if (++calls === 2) expired.resolve();
    return false;
  });
  await subject.stop(Context.background());
  await bounded(expired.promise);
  await delay(0);
  assert.equal(calls, 2);
  assert.equal(subject.size(), 0);
});

await test("join storage stop does not wait for a running expiry callback", async () => {
  const subject = storage(10);
  const entered = gate();
  const release = gate();
  const finished = gate();
  let calls = 0;
  try {
    await subject.joinValue(new MessageContext(), "key", 0, "value", async () => {
      if (++calls === 2) {
        entered.resolve();
        await release.promise;
        finished.resolve();
      }
      return false;
    });
    await bounded(entered.promise);
    await bounded(subject.stop(Context.background()));
    assert.equal(subject.size(), 1);
  } finally {
    release.resolve();
    await bounded(finished.promise);
    await subject.stop(Context.background());
  }
  await delay(0);
  assert.equal(subject.size(), 0);
});

await test("join storage stop does not wait for a zero-TTL join callback", async () => {
  const subject = storage(0);
  const entered = gate();
  const release = gate();
  const admitted = subject.joinValue(new MessageContext(), "key", 0, "value", async () => {
    entered.resolve();
    await release.promise;
    return true;
  });
  try {
    await entered.promise;
    await bounded(subject.stop(Context.background()));
  } finally {
    release.resolve();
    await admitted;
    await subject.stop(Context.background());
  }
});

await test("join storage TTL admissions wait behind a pending maintenance stop", async () => {
  const subject = storage(60_000);
  const entered = gate();
  const release = gate();
  let stopping: Promise<void> | undefined;
  let next: Promise<void> | undefined;
  let nextEntered = false;
  const admitted = subject.joinValue(new MessageContext(), "first", 0, 1, async () => {
    entered.resolve();
    await release.promise;
    return true;
  });
  try {
    await entered.promise;
    stopping = subject.stop(Context.background());
    next = subject.joinValue(new MessageContext(), "next", 0, 2, () => {
      nextEntered = true;
      return true;
    });
    void next.catch(() => undefined);
    await delay(0);
    assert.equal(nextEntered, false);
    release.resolve();
    await bounded(Promise.all([admitted, stopping, next]));
    assert.equal(nextEntered, true);
    assert.equal(subject.size(), 0);
  } finally {
    release.resolve();
    await Promise.allSettled([admitted, stopping, next]);
    await subject.stop(Context.background());
  }
});
