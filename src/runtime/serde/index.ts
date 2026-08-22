/** Serialization contracts are exported from this module. */
export {};
export * from "./bytes.js";
export * from "./collection.js";
export * from "./defaults.js";
export * from "./json.js";
export * from "./protobuf.js";
export * from "./registry.js";
export * from "./scalar.js";
export * from "./stream.js";
export { SerdeError, StubSerde, unlimitedSerdeLimits } from "./serde.js";
export type { Serde, SerdeLimits, StreamSerde } from "./serde.js";
