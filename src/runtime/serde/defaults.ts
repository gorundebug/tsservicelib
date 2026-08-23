import { BytesSerde, StringSerde } from "./bytes.js";
import { JsonSerde } from "./json.js";
import { isScheduleTrigger, type ScheduleTrigger } from "../schedule.js";
import {
  BoolArraySerde,
  Float32ArraySerde,
  Float64ArraySerde,
  Int8ArraySerde,
  Int16ArraySerde,
  Int32ArraySerde,
  Int64ArraySerde,
  IntArraySerde,
  StringArraySerde,
  UInt8ArraySerde,
  UInt16ArraySerde,
  UInt32ArraySerde,
  UInt64ArraySerde,
  UIntArraySerde
} from "./collection.js";
import {
  BoolSerde,
  Float32Serde,
  Float64Serde,
  Int8Serde,
  Int16Serde,
  Int32Serde,
  Int64Serde,
  IntSerde,
  RuneSerde,
  UInt8Serde,
  UInt16Serde,
  UInt32Serde,
  UInt64Serde,
  UIntSerde
} from "./scalar.js";
import { SerdeRegistry, SerdeType } from "./registry.js";
import { StubSerde } from "./serde.js";
import { makeStreamSerde } from "./stream.js";

export const boolSerdeType = new SerdeType("bool", (value): value is boolean => isBoolean(value));
export const int8SerdeType = new SerdeType("int8", isInt8);
export const int16SerdeType = new SerdeType("int16", isInt16);
export const int32SerdeType = new SerdeType("int32", isInt32);
export const int64SerdeType = new SerdeType("int64", isInt64);
export const intSerdeType = new SerdeType("int", isInt64);
export const uint8SerdeType = new SerdeType("uint8", isUInt8);
export const uint16SerdeType = new SerdeType("uint16", isUInt16);
export const uint32SerdeType = new SerdeType("uint32", isUInt32);
export const uint64SerdeType = new SerdeType("uint64", isUInt64);
export const uintSerdeType = new SerdeType("uint", isUInt64);
export const float32SerdeType = new SerdeType("float32", (value): value is number =>
  isNumber(value)
);
export const float64SerdeType = new SerdeType("float64", (value): value is number =>
  isNumber(value)
);
export const runeSerdeType = new SerdeType("rune", isInt32);
export const stringSerdeType = new SerdeType("string", (value): value is string => isString(value));
export const bytesSerdeType = new SerdeType("[]byte", (value): value is Uint8Array =>
  isBytes(value)
);
export const errorSerdeType = new SerdeType(
  "error",
  (value): value is Error => value instanceof Error
);
export const scheduleTriggerSerdeType = new SerdeType<ScheduleTrigger>(
  "schedule trigger",
  isScheduleTrigger
);
export const boolArraySerdeType = makeArrayType("[]bool", boolSerdeType);
export const int8ArraySerdeType = makeArrayType("[]int8", int8SerdeType);
export const int16ArraySerdeType = makeArrayType("[]int16", int16SerdeType);
export const int32ArraySerdeType = makeArrayType("[]int32", int32SerdeType);
export const int64ArraySerdeType = makeArrayType("[]int64", int64SerdeType);
export const intArraySerdeType = makeArrayType("[]int", intSerdeType);
export const uint8ArraySerdeType = makeArrayType("[]uint8", uint8SerdeType);
export const uint16ArraySerdeType = makeArrayType("[]uint16", uint16SerdeType);
export const uint32ArraySerdeType = makeArrayType("[]uint32", uint32SerdeType);
export const uint64ArraySerdeType = makeArrayType("[]uint64", uint64SerdeType);
export const uintArraySerdeType = makeArrayType("[]uint", uintSerdeType);
export const float32ArraySerdeType = makeArrayType("[]float32", float32SerdeType);
export const float64ArraySerdeType = makeArrayType("[]float64", float64SerdeType);
export const stringArraySerdeType = makeArrayType("[]string", stringSerdeType);

export function makeDefaultSerdeRegistry(): SerdeRegistry {
  const registry = new SerdeRegistry();
  registry.register(boolSerdeType, makeStreamSerde(new BoolSerde()));
  registry.register(int8SerdeType, makeStreamSerde(new Int8Serde()));
  registry.register(int16SerdeType, makeStreamSerde(new Int16Serde()));
  registry.register(int32SerdeType, makeStreamSerde(new Int32Serde()));
  registry.register(int64SerdeType, makeStreamSerde(new Int64Serde()));
  registry.register(intSerdeType, makeStreamSerde(new IntSerde()));
  registry.register(uint8SerdeType, makeStreamSerde(new UInt8Serde()));
  registry.register(uint16SerdeType, makeStreamSerde(new UInt16Serde()));
  registry.register(uint32SerdeType, makeStreamSerde(new UInt32Serde()));
  registry.register(uint64SerdeType, makeStreamSerde(new UInt64Serde()));
  registry.register(uintSerdeType, makeStreamSerde(new UIntSerde()));
  registry.register(float32SerdeType, makeStreamSerde(new Float32Serde()));
  registry.register(float64SerdeType, makeStreamSerde(new Float64Serde()));
  registry.register(runeSerdeType, makeStreamSerde(new RuneSerde()));
  registry.register(stringSerdeType, makeStreamSerde(new StringSerde()));
  registry.register(bytesSerdeType, makeStreamSerde(new BytesSerde()));
  registry.register(errorSerdeType, makeStreamSerde(new StubSerde<Error>()));
  registry.register(
    scheduleTriggerSerdeType,
    makeStreamSerde(new JsonSerde(scheduleTriggerSerdeType))
  );
  registry.register(boolArraySerdeType, makeStreamSerde(new BoolArraySerde()));
  registry.register(int8ArraySerdeType, makeStreamSerde(new Int8ArraySerde()));
  registry.register(int16ArraySerdeType, makeStreamSerde(new Int16ArraySerde()));
  registry.register(int32ArraySerdeType, makeStreamSerde(new Int32ArraySerde()));
  registry.register(int64ArraySerdeType, makeStreamSerde(new Int64ArraySerde()));
  registry.register(intArraySerdeType, makeStreamSerde(new IntArraySerde()));
  registry.register(uint8ArraySerdeType, makeStreamSerde(new UInt8ArraySerde()));
  registry.register(uint16ArraySerdeType, makeStreamSerde(new UInt16ArraySerde()));
  registry.register(uint32ArraySerdeType, makeStreamSerde(new UInt32ArraySerde()));
  registry.register(uint64ArraySerdeType, makeStreamSerde(new UInt64ArraySerde()));
  registry.register(uintArraySerdeType, makeStreamSerde(new UIntArraySerde()));
  registry.register(float32ArraySerdeType, makeStreamSerde(new Float32ArraySerde()));
  registry.register(float64ArraySerdeType, makeStreamSerde(new Float64ArraySerde()));
  registry.register(stringArraySerdeType, makeStreamSerde(new StringArraySerde()));
  return registry;
}

function makeArrayType<T>(name: string, elementType: SerdeType<T>): SerdeType<readonly T[]> {
  return new SerdeType(
    name,
    (value): value is readonly T[] =>
      Array.isArray(value) && value.every((item: unknown) => elementType.is(item))
  );
}

function isBoolean(value: unknown): value is boolean {
  return typeof value === "boolean";
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isNumber(value: unknown): value is number {
  return typeof value === "number";
}

function isBytes(value: unknown): value is Uint8Array {
  return value instanceof Uint8Array;
}

function isIntegerInRange(value: unknown, minimum: number, maximum: number): value is number {
  return (
    typeof value === "number" && Number.isInteger(value) && value >= minimum && value <= maximum
  );
}

function isInt8(value: unknown): value is number {
  return isIntegerInRange(value, -0x80, 0x7f);
}

function isInt16(value: unknown): value is number {
  return isIntegerInRange(value, -0x8000, 0x7fff);
}

function isInt32(value: unknown): value is number {
  return isIntegerInRange(value, -0x80000000, 0x7fffffff);
}

function isUInt8(value: unknown): value is number {
  return isIntegerInRange(value, 0, 0xff);
}

function isUInt16(value: unknown): value is number {
  return isIntegerInRange(value, 0, 0xffff);
}

function isUInt32(value: unknown): value is number {
  return isIntegerInRange(value, 0, 0xffffffff);
}

function isInt64(value: unknown): value is bigint {
  return typeof value === "bigint" && value >= -(1n << 63n) && value < 1n << 63n;
}

function isUInt64(value: unknown): value is bigint {
  return typeof value === "bigint" && value >= 0n && value < 1n << 64n;
}
