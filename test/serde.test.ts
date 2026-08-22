import assert from "node:assert/strict";
import { test } from "node:test";

import {
  ArraySerde,
  BoolSerde,
  BoolArraySerde,
  BytesSerde,
  Float32Serde,
  Float64Serde,
  Int8Serde,
  Int16Serde,
  Int32Serde,
  Int64Serde,
  Int16ArraySerde,
  Int32ArraySerde,
  Int64ArraySerde,
  IntSerde,
  errorSerdeType,
  makeDefaultSerdeRegistry,
  MapSerde,
  RuneSerde,
  SerdeError,
  StreamKeyValueSerde,
  StringArraySerde,
  StringSerde,
  StubSerde,
  UInt8Serde,
  UInt16Serde,
  UInt32Serde,
  UInt64Serde,
  UIntSerde,
  UInt64ArraySerde,
  makeStreamSerde,
  unlimitedSerdeLimits
} from "@gorundebug/tsservicelib/runtime/serde";

function bytes(hex: string): Uint8Array {
  return Buffer.from(hex.replaceAll(" ", ""), "hex");
}

function hex(value: Uint8Array): string {
  return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("hex");
}

await test("primitive serde is byte-compatible with the Go wire format", () => {
  const fixtures: readonly (readonly [string, Uint8Array])[] = [
    [hex(new UInt8Serde().serialize(0xab)), bytes("ab")],
    [hex(new UInt16Serde().serialize(0xabcd)), bytes("abcd")],
    [hex(new UInt32Serde().serialize(0x89abcdef)), bytes("89abcdef")],
    [hex(new UInt64Serde().serialize(0x0123456789abcdefn)), bytes("0123456789abcdef")],
    [hex(new UIntSerde().serialize(0xffffffffffffffffn)), bytes("ffffffffffffffff")],
    [hex(new Int8Serde().serialize(-1)), bytes("ff")],
    [hex(new Int16Serde().serialize(-1)), bytes("7fff")],
    [hex(new Int16Serde().serialize(0)), bytes("8000")],
    [hex(new Int32Serde().serialize(-1)), bytes("7fffffff")],
    [hex(new Int32Serde().serialize(0)), bytes("80000000")],
    [hex(new Int64Serde().serialize(-1n)), bytes("7fffffffffffffff")],
    [hex(new IntSerde().serialize(0n)), bytes("8000000000000000")],
    [hex(new RuneSerde().serialize(0x1f642)), bytes("0001f642")],
    [hex(new BoolSerde().serialize(false)), bytes("00")],
    [hex(new BoolSerde().serialize(true)), bytes("01")],
    [hex(new Float32Serde().serialize(1.5)), bytes("3fc00000")],
    [hex(new Float64Serde().serialize(-2.5)), bytes("c004000000000000")]
  ];

  for (const [actual, expected] of fixtures) {
    assert.equal(actual, hex(expected));
  }
});

await test("primitive serde validates ranges and preserves special floats", () => {
  assert.throws(() => new UInt8Serde().serialize(256), RangeError);
  assert.throws(() => new Int16Serde().serialize(1.5), RangeError);
  assert.throws(() => new Int64Serde().serialize(1n << 63n), RangeError);
  assert.throws(() => new UInt64Serde().serialize(-1n), RangeError);

  const float64 = new Float64Serde();
  assert.equal(float64.deserialize(float64.serialize(Number.POSITIVE_INFINITY)), Infinity);
  assert.equal(float64.deserialize(float64.serialize(Number.NEGATIVE_INFINITY)), -Infinity);
  assert.ok(Number.isNaN(float64.deserialize(float64.serialize(Number.NaN))));
  assert.ok(Object.is(float64.deserialize(float64.serialize(-0)), -0));
});

await test("default error serde is typed and deliberately non-serializing", () => {
  const serde = makeDefaultSerdeRegistry().require(errorSerdeType);
  assert.throws(() => serde.serialize(new Error("failure")), SerdeError);
  assert.throws(() => {
    errorSerdeType.assert("failure");
  }, TypeError);
});

await test("string and bytes serde use uint64 framing and append semantics", () => {
  const stringSerde = new StringSerde();
  const bytesSerde = new BytesSerde();
  assert.equal(hex(stringSerde.serialize("Привет")), "000000000000000cd09fd180d0b8d0b2d0b5d182");
  assert.equal(hex(bytesSerde.serialize(bytes("deadbeef"))), "0000000000000004deadbeef");
  assert.equal(hex(new UInt16Serde().serialize(2, bytes("aabb"))), "aabb0002");
  assert.equal(stringSerde.deserialize(stringSerde.serialize("🙂")), "🙂");

  const encoded = bytesSerde.serialize(bytes("010203"));
  const decoded = bytesSerde.deserialize(encoded);
  assert.equal(decoded.buffer, encoded.buffer);
  assert.equal(decoded.byteOffset, 8);
});

await test("serde reports exact frame offsets and enforces configured limits", () => {
  const limits = {
    ...unlimitedSerdeLimits,
    maxStringBytes: 3,
    maxBytes: 2,
    maxTotalBytes: 16
  };
  assert.throws(() => new StringSerde(limits).serialize("four"), /string exceeds configured limit/);
  assert.throws(
    () => new BytesSerde(limits).serialize(bytes("000102")),
    /bytes exceed configured limit/
  );

  assert.throws(
    () => new StringSerde().deserialize(bytes("0000000000000004ff")),
    (error: unknown) =>
      error instanceof SerdeError &&
      error.offset === 8 &&
      error.message.includes("underflow while reading string")
  );
  assert.throws(
    () => new StringSerde().deserialize(bytes("0000000000000001ff")),
    (error: unknown) =>
      error instanceof SerdeError && error.offset === 8 && error.message.includes("invalid UTF-8")
  );
  assert.throws(
    () => new UInt32Serde().deserialize(bytes("000102")),
    (error: unknown) => error instanceof SerdeError && error.offset === 0
  );
});

await test("stream serde preserves value and KeyValue framing semantics", () => {
  const valueSerde = makeStreamSerde(new StringSerde());
  assert.equal(valueSerde.isKeyValue(), false);
  assert.equal(valueSerde.serializeKey("value"), undefined);
  assert.equal(
    valueSerde.deserializeKeyValue(undefined, valueSerde.serializeValue("value")),
    "value"
  );

  const serde = new StreamKeyValueSerde(new StringSerde(), new Int32Serde());
  const value = { key: "a", value: -1 };
  assert.equal(
    hex(serde.serialize(value)),
    "000000000000000900000000000000016100000000000000047fffffff"
  );
  assert.deepEqual(serde.deserialize(serde.serialize(value)), value);
  assert.deepEqual(
    serde.deserializeKeyValue(serde.serializeKey(value), serde.serializeValue(value)),
    value
  );
  assert.throws(() => serde.deserializeKeyValue(undefined, bytes("00")), /key is required/);
});

await test("fixed-size arrays preserve the canonical packed element wire format", () => {
  const int16 = new Int16ArraySerde();
  assert.equal(hex(int16.serialize([-1, 0, 1])), "00000000000000037fff80008001");
  assert.deepEqual(int16.deserialize(int16.serialize([-32768, 0, 32767])), [-32768, 0, 32767]);

  const int32 = new Int32ArraySerde();
  assert.equal(hex(int32.serialize([-1, 0, 1])), "00000000000000037fffffff8000000080000001");
  const int64 = new Int64ArraySerde();
  assert.deepEqual(int64.deserialize(int64.serialize([-1n, 0n, 1n])), [-1n, 0n, 1n]);
  const uint64 = new UInt64ArraySerde();
  assert.deepEqual(uint64.deserialize(uint64.serialize([0n, 0xffffffffffffffffn])), [
    0n,
    0xffffffffffffffffn
  ]);
  const bool = new BoolArraySerde();
  assert.equal(hex(bool.serialize([true, false, true])), "0000000000000003010001");
});

await test("string, generic and nested arrays preserve their distinct framing", () => {
  const strings = new StringArraySerde();
  assert.equal(
    hex(strings.serialize(["a", "🙂"])),
    "00000000000000020000000000000001610000000000000004f09f9982"
  );
  assert.deepEqual(strings.deserialize(strings.serialize(["a", "🙂"])), ["a", "🙂"]);

  const genericStrings = new ArraySerde(new StringSerde());
  assert.equal(
    hex(genericStrings.serialize(["a"])),
    "00000000000000010000000000000009000000000000000161"
  );

  const nested = new ArraySerde(new Int16ArraySerde());
  const value = [[1, 2], [-1]];
  assert.deepEqual(nested.deserialize(nested.serialize(value)), value);
});

await test("map serde frames parallel key/value arrays and validates their cardinality", () => {
  const serde = new MapSerde(new StringArraySerde(), new Int32ArraySerde());
  const value = new Map([
    ["a", -1],
    ["b", 2]
  ]);
  assert.equal(
    hex(serde.serialize(value)),
    "000000000000001a0000000000000002000000000000000161000000000000000162" +
      "000000000000001000000000000000027fffffff80000002"
  );
  assert.deepEqual([...serde.deserialize(serde.serialize(value))], [...value]);

  const mismatched = bytes(
    "00000000000000110000000000000001000000000000000161" + "00000000000000080000000000000000"
  );
  assert.throws(
    () => serde.deserialize(mismatched),
    (error: unknown) => error instanceof SerdeError && error.message.includes("counts do not match")
  );
});

await test("collection serde applies count, total-byte and element limits before allocation", () => {
  const limits = {
    ...unlimitedSerdeLimits,
    maxContainerElements: 1,
    maxTotalBytes: 32
  };
  const serde = new ArraySerde(new Int32Serde(), limits);
  assert.throws(() => serde.serialize([1, 2]), /array exceeds configured element limit/);
  assert.throws(
    () => serde.deserialize(bytes("0000000000000002")),
    (error: unknown) =>
      error instanceof SerdeError &&
      error.offset === 0 &&
      error.message.includes("array count exceeds configured limit")
  );
});

await test("stub serde remains explicitly unusable", () => {
  const serde = new StubSerde<string>();
  assert.equal(serde.isStub(), true);
  assert.throws(() => serde.serialize("value"), /stub serde cannot serialize/);
  assert.throws(() => serde.deserialize(bytes("00")), /stub serde cannot deserialize/);
});
