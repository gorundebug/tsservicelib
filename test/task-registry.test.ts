import assert from "node:assert/strict";
import { test } from "node:test";

import {
  RuntimeDrainTimeoutError,
  RuntimeStoppedError,
  RuntimeTaskRegistry
} from "@gorundebug/tsservicelib/runtime";

await test("runtime task registry observes failures and drains admitted work", async () => {
  const errors: Error[] = [];
  const registry = new RuntimeTaskRegistry((error) => errors.push(error));
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });

  const accepted = registry.admit(async () => gate);
  const rejected = registry.admit(() => Promise.reject(new Error("task failure")));
  await assert.rejects(rejected, /task failure/);
  registry.stopAdmission();
  await assert.rejects(
    registry.admit(() => Promise.resolve()),
    RuntimeStoppedError
  );
  assert.equal(registry.activeCount(), 1);
  release?.();
  await accepted;
  await registry.drain(100);
  assert.equal(registry.activeCount(), 0);
  assert.deepEqual(
    errors.map(({ message }) => message),
    ["task failure"]
  );
});

await test("runtime task registry observes detached failures exactly once", async () => {
  const errors: Error[] = [];
  const registry = new RuntimeTaskRegistry((error) => errors.push(error));

  registry.admitDetached(() => Promise.reject(new Error("detached failure")));
  await registry.drain();

  assert.deepEqual(
    errors.map(({ message }) => message),
    ["detached failure"]
  );
  assert.equal(registry.activeCount(), 0);
});

await test("runtime task registry contains failures from its terminal error reporter", async () => {
  const registry = new RuntimeTaskRegistry(() => {
    throw new Error("reporter failure");
  });

  registry.admitDetached(() => Promise.reject(new Error("task failure")));
  await registry.drain();

  assert.equal(registry.activeCount(), 0);
});

await test("runtime task registry reports bounded drain timeout", async () => {
  const registry = new RuntimeTaskRegistry();
  const cancelled = registry.admit(
    async (signal) =>
      new Promise<void>((resolve) => {
        signal.addEventListener(
          "abort",
          () => {
            resolve();
          },
          { once: true }
        );
      })
  );
  registry.stopAdmission();
  await assert.rejects(registry.drain(5), RuntimeDrainTimeoutError);
  registry.cancel();
  await cancelled;
  await registry.drain(100);
  assert.equal(registry.activeCount(), 0);
});

await test("runtime drain applies one timeout budget across nested task generations", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const registry = new RuntimeTaskRegistry();
  let releaseParent: (() => void) | undefined;
  const parentGate = new Promise<void>((resolve) => {
    releaseParent = resolve;
  });
  let child: Promise<void> | undefined;
  const parent = registry.admit(async () => {
    await parentGate;
    child = registry.admit(
      async (signal) =>
        new Promise<void>((resolve) => {
          signal.addEventListener(
            "abort",
            () => {
              resolve();
            },
            { once: true }
          );
        })
    );
  });

  const draining = registry.drain(5);
  t.mock.timers.tick(4);
  releaseParent?.();
  await parent;
  await Promise.resolve();
  assert.equal(registry.activeCount(), 1);

  t.mock.timers.tick(1);
  await assert.rejects(draining, RuntimeDrainTimeoutError);
  registry.cancel();
  await child;
  await registry.drain();
  assert.equal(registry.activeCount(), 0);
});
