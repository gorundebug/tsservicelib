import type {
  AnyStreamConfig,
  CaseStreamConfig,
  CycleLinkStreamConfig,
  DelayStreamConfig,
  FilterStreamConfig,
  FlatMapIterableStreamConfig,
  FlatMapStreamConfig,
  InputStreamConfig,
  JoinStreamConfig,
  KeyByStreamConfig,
  MapStreamConfig,
  MergeStreamConfig,
  MultiJoinStreamConfig,
  ProcessStreamConfig,
  SinkStreamConfig,
  SplitStreamConfig,
  StreamConfig,
  WhenStreamConfig
} from "./types.js";

type StreamGuard<T extends AnyStreamConfig> = (value: StreamConfig) => value is T;

function hasString(value: StreamConfig, key: string): boolean {
  return key in value && typeof Reflect.get(value, key) === "string";
}

function hasNumber(value: StreamConfig, key: string): boolean {
  const field: unknown = Reflect.get(value, key);
  return key in value && typeof field === "number" && Number.isFinite(field);
}

function hasBoolean(value: StreamConfig, key: string): boolean {
  return key in value && typeof Reflect.get(value, key) === "boolean";
}

function requireStream<T extends AnyStreamConfig>(
  value: StreamConfig | undefined,
  name: string,
  guard: StreamGuard<T>
): T {
  if (value === undefined || !guard(value)) {
    throw new Error(`invalid ${name} stream config`);
  }
  return value;
}

export function isInputStreamConfig(value: StreamConfig): value is InputStreamConfig {
  return value.type === "Input" && hasString(value, "valueType") && hasNumber(value, "idEndpoint");
}

export function requireInputStreamConfig(value: StreamConfig | undefined): InputStreamConfig {
  return requireStream(value, "Input", isInputStreamConfig);
}

export function isMapStreamConfig(value: StreamConfig): value is MapStreamConfig {
  return value.type === "Map" && hasString(value, "valueType");
}

export function requireMapStreamConfig(value: StreamConfig | undefined): MapStreamConfig {
  return requireStream(value, "Map", isMapStreamConfig);
}

export function isFilterStreamConfig(value: StreamConfig): value is FilterStreamConfig {
  return value.type === "Filter";
}

export function requireFilterStreamConfig(value: StreamConfig | undefined): FilterStreamConfig {
  return requireStream(value, "Filter", isFilterStreamConfig);
}

export function isJoinStreamConfig(value: StreamConfig): value is JoinStreamConfig {
  return (
    value.type === "Join" &&
    hasString(value, "valueType") &&
    hasNumber(value, "joinType") &&
    hasNumber(value, "joinStorage") &&
    hasNumber(value, "ttl") &&
    hasBoolean(value, "renewTTL")
  );
}

export function requireJoinStreamConfig(value: StreamConfig | undefined): JoinStreamConfig {
  return requireStream(value, "Join", isJoinStreamConfig);
}

export function isMultiJoinStreamConfig(value: StreamConfig): value is MultiJoinStreamConfig {
  return (
    value.type === "MultiJoin" &&
    hasString(value, "valueType") &&
    hasNumber(value, "joinStorage") &&
    hasNumber(value, "ttl") &&
    hasBoolean(value, "renewTTL")
  );
}

export function requireMultiJoinStreamConfig(
  value: StreamConfig | undefined
): MultiJoinStreamConfig {
  return requireStream(value, "MultiJoin", isMultiJoinStreamConfig);
}

export function isProcessStreamConfig(value: StreamConfig): value is ProcessStreamConfig {
  return value.type === "Process";
}

export function requireProcessStreamConfig(value: StreamConfig | undefined): ProcessStreamConfig {
  return requireStream(value, "Process", isProcessStreamConfig);
}

export function isFlatMapStreamConfig(value: StreamConfig): value is FlatMapStreamConfig {
  return value.type === "FlatMap" && hasString(value, "valueType");
}

export function requireFlatMapStreamConfig(value: StreamConfig | undefined): FlatMapStreamConfig {
  return requireStream(value, "FlatMap", isFlatMapStreamConfig);
}

export function isFlatMapIterableStreamConfig(
  value: StreamConfig
): value is FlatMapIterableStreamConfig {
  return value.type === "FlatMapIterable" && hasString(value, "valueType");
}

export function requireFlatMapIterableStreamConfig(
  value: StreamConfig | undefined
): FlatMapIterableStreamConfig {
  return requireStream(value, "FlatMapIterable", isFlatMapIterableStreamConfig);
}

export function isKeyByStreamConfig(value: StreamConfig): value is KeyByStreamConfig {
  return value.type === "KeyBy" && hasString(value, "keyType") && hasString(value, "valueType");
}

export function requireKeyByStreamConfig(value: StreamConfig | undefined): KeyByStreamConfig {
  return requireStream(value, "KeyBy", isKeyByStreamConfig);
}

export function isMergeStreamConfig(value: StreamConfig): value is MergeStreamConfig {
  return value.type === "Merge";
}

export function requireMergeStreamConfig(value: StreamConfig | undefined): MergeStreamConfig {
  return requireStream(value, "Merge", isMergeStreamConfig);
}

export function isSplitStreamConfig(value: StreamConfig): value is SplitStreamConfig {
  return value.type === "Split";
}

export function requireSplitStreamConfig(value: StreamConfig | undefined): SplitStreamConfig {
  return requireStream(value, "Split", isSplitStreamConfig);
}

export function isCaseStreamConfig(value: StreamConfig): value is CaseStreamConfig {
  return value.type === "Case";
}

export function requireCaseStreamConfig(value: StreamConfig | undefined): CaseStreamConfig {
  return requireStream(value, "Case", isCaseStreamConfig);
}

export function isSinkStreamConfig(value: StreamConfig): value is SinkStreamConfig {
  return value.type === "Sink" && hasNumber(value, "idEndpoint");
}

export function requireSinkStreamConfig(value: StreamConfig | undefined): SinkStreamConfig {
  return requireStream(value, "Sink", isSinkStreamConfig);
}

export function isCycleLinkStreamConfig(value: StreamConfig): value is CycleLinkStreamConfig {
  return value.type === "CycleLink";
}

export function requireCycleLinkStreamConfig(
  value: StreamConfig | undefined
): CycleLinkStreamConfig {
  return requireStream(value, "CycleLink", isCycleLinkStreamConfig);
}

export function isDelayStreamConfig(value: StreamConfig): value is DelayStreamConfig {
  return value.type === "Delay" && hasNumber(value, "duration");
}

export function requireDelayStreamConfig(value: StreamConfig | undefined): DelayStreamConfig {
  return requireStream(value, "Delay", isDelayStreamConfig);
}

export function isWhenStreamConfig(value: StreamConfig): value is WhenStreamConfig {
  return value.type === "When" && hasString(value, "valueType");
}

export function requireWhenStreamConfig(value: StreamConfig | undefined): WhenStreamConfig {
  return requireStream(value, "When", isWhenStreamConfig);
}
