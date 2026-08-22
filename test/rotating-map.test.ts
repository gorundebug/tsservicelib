import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";

import {
  Context,
  DuplicateKeyError,
  RotatingMap,
  StoreAlreadyStartedError,
  StoreStoppedError
} from "@gorundebug/tsservicelib/runtime";

class TestRotatingMap<K, V> extends RotatingMap<K, V> {
  public rotations = 0;

  public rotateNow(): void {
    this.rotate();
  }

  protected override rotate(): void {
    this.rotations += 1;
    super.rotate();
  }
}

await test("rotating map preserves lookup, duplicate and pop semantics", () => {
  const map = new TestRotatingMap<string, number>(60_000, 0);
  map.set("first", 1);
  assert.deepEqual(map.get("first"), [1, true]);
  assert.throws(() => {
    map.set("first", 2);
  }, DuplicateKeyError);

  map.rotateNow();
  assert.deepEqual(map.get("first"), [1, true]);
  assert.throws(() => {
    map.set("first", 2);
  }, DuplicateKeyError);

  map.set("second", 2);
  map.rotateNow();
  assert.deepEqual(map.get("first"), [1, true]);
  assert.deepEqual(map.get("second"), [2, true]);
  assert.equal(map.size(), 2);
  assert.deepEqual(map.pop("first"), [1, true]);
  assert.deepEqual(map.pop("first"), [undefined, false]);
  assert.deepEqual(map.get("missing"), [undefined, false]);
});

await test("getOrCreate is synchronous, atomic and supports undefined values", () => {
  const map = new RotatingMap<string, undefined>(60_000, 0);
  let calls = 0;
  const factory = (): undefined => {
    calls += 1;
    return undefined;
  };

  assert.deepEqual(map.getOrCreate("key", factory), [undefined, false]);
  assert.deepEqual(map.getOrCreate("key", factory), [undefined, true]);
  assert.deepEqual(map.get("key"), [undefined, true]);
  assert.equal(calls, 1);
});

await test("rotating map timer lifecycle is strict and stop is idempotent", async () => {
  const map = new TestRotatingMap<string, number>(2, 0);
  const context = Context.background();
  map.start(context);
  assert.throws(() => {
    map.start(context);
  }, StoreAlreadyStartedError);

  for (let attempt = 0; attempt < 20 && map.rotations === 0; attempt += 1) {
    await delay(2);
  }
  assert.ok(map.rotations > 0);
  map.stop(context);
  map.stop(context);
  const stoppedAt = map.rotations;
  await delay(5);
  assert.equal(map.rotations, stoppedAt);
  assert.throws(() => {
    map.start(context);
  }, StoreStoppedError);
});

await test("rotating map validates construction bounds", () => {
  assert.throws(() => new RotatingMap(0), /interval must be positive/);
  assert.throws(() => new RotatingMap(1, -1), /minimum capacity/);
});
