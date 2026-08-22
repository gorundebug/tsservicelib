import assert from "node:assert/strict";
import { test } from "node:test";

import { create, isMessage } from "@bufbuild/protobuf";
import { TimestampSchema } from "@bufbuild/protobuf/wkt";
import {
  JsonSerde,
  ProtobufSerde,
  SerdeError,
  SerdeRegistry,
  SerdeType,
  makeStreamSerde
} from "@gorundebug/tsservicelib/runtime/serde";

interface Product {
  readonly id: string;
  readonly note?: string | null;
}

const productType = new SerdeType<Product>("example.Product", isProduct);

await test("JSON serde validates generated runtime shape and preserves absent versus null", () => {
  const serde = new JsonSerde(productType);
  const absent: Product = { id: "one" };
  const nullable: Product = { id: "one", note: null };

  assert.equal(text(serde.serialize(absent)), '{"id":"one"}');
  assert.equal(text(serde.serialize(nullable)), '{"id":"one","note":null}');
  assert.equal(Object.hasOwn(serde.deserialize(serde.serialize(absent)), "note"), false);
  assert.equal(serde.deserialize(serde.serialize(nullable)).note, null);
  assert.throws(() => serde.deserialize(encoded('{"id":1}')), /value is not example.Product/);
  assert.throws(() => serde.deserialize(encoded("{")), SerdeError);
  assert.throws(() => serde.deserialize(Uint8Array.of(0xff)), /not valid UTF-8/);
});

await test("protobuf serde uses Protobuf-ES binary codecs and exact bigint fields", () => {
  const serde = new ProtobufSerde(TimestampSchema);
  const value = create(TimestampSchema, { seconds: 150n, nanos: 123 });
  assert.equal(hex(serde.serialize(value)), "089601107b");
  assert.deepEqual(serde.deserialize(serde.serialize(value)), value);
  assert.equal(hex(serde.serialize(value, Uint8Array.of(0xaa))), "aa089601107b");
  assert.throws(() => new ProtobufSerde(TimestampSchema, 1).serialize(value), /configured limit/);
  assert.throws(() => serde.deserialize(Uint8Array.of(0x80)), /protobuf deserialization failed/);
});

await test("serde registry uses runtime-validated typed keys without erased public casts", () => {
  const registry = new SerdeRegistry();
  registry.register(productType, makeStreamSerde(new JsonSerde(productType)));
  const serde = registry.require(productType);
  assert.deepEqual(serde.deserialize(serde.serialize({ id: "registered" })), {
    id: "registered"
  });

  const timestampType = new SerdeType(
    "google.protobuf.Timestamp",
    (value): value is ReturnType<typeof createTimestamp> => isMessage(value, TimestampSchema)
  );
  const protobuf = new ProtobufSerde(TimestampSchema);
  registry.register(timestampType, makeStreamSerde(protobuf));
  assert.equal(
    registry.require(timestampType).deserialize(protobuf.serialize(createTimestamp())).seconds,
    1n
  );

  assert.throws(() => {
    registry.register(productType, makeStreamSerde(new JsonSerde(productType)));
  }, /already registered/);
  const duplicateName = new SerdeType<Product>("example.Product", isProduct);
  assert.throws(() => {
    registry.register(duplicateName, makeStreamSerde(new JsonSerde(duplicateName)));
  }, /already registered/);
  assert.throws(
    () => registry.require(new SerdeType<Product>("example.Missing", isProduct)),
    /not registered/
  );
});

function createTimestamp() {
  return create(TimestampSchema, { seconds: 1n });
}

function isProduct(value: unknown): value is Product {
  if (typeof value !== "object" || value === null || !("id" in value)) {
    return false;
  }
  if (typeof value.id !== "string") {
    return false;
  }
  if (!Object.hasOwn(value, "note") || !("note" in value)) {
    return true;
  }
  return value.note === null || typeof value.note === "string";
}

function encoded(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function text(value: Uint8Array): string {
  return new TextDecoder().decode(value);
}

function hex(value: Uint8Array): string {
  return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("hex");
}
