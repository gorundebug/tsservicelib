import type { Collector } from "../runtime/collector.js";
import type { MessageContext } from "../runtime/context.js";
import type { KeyValue } from "../runtime/datastruct/key-value.js";
import type { Completion, Stream } from "../runtime/stream.js";

export interface When {
  readonly name: string;
  valueType(): string;
  whenConsumer(): Stream;
}

/** Resolve the When node identity once in buildSwitch, not for every message. */
export function whenIndex(whenItems: readonly When[], name: string): number {
  if (name.length === 0) throw new Error("case branch name must not be empty");
  let index = -1;
  for (const [candidate, item] of whenItems.entries()) {
    if (item.name !== name) continue;
    if (index >= 0) {
      throw new Error(`case branch ${JSON.stringify(name)} is ambiguous (indices ${String(index)} and ${String(candidate)})`);
    }
    index = candidate;
  }
  if (index < 0) throw new Error(`case branch ${JSON.stringify(name)} was not found`);
  return index;
}

export interface BuildSwitchFunction<T> {
  buildSwitch(stream: Stream, whenItems: readonly When[]): (value: Readonly<T>) => number;
}

/** JavaScript equivalent of Go's runtime-type Case selector. */
export function defaultBuildSwitch(
  stream: Stream,
  whenItems: readonly When[]
): (value: unknown) => number {
  const registry = stream.runtimeEnvironment().serdeRegistry();
  return (value: unknown): number => {
    let selected: number | undefined;
    for (const [index, when] of whenItems.entries()) {
      if (registry.matchesByName(when.valueType(), value)) selected = index;
    }
    if (selected === undefined) {
      throw new TypeError(`unknown value type in case switch for stream ${stream.name}`);
    }
    return selected;
  };
}

export interface FilterFunction<T> {
  filter(context: MessageContext, stream: Stream, value: Readonly<T>): boolean | Promise<boolean>;
}

export interface DelayFunction<T> {
  duration(context: MessageContext, stream: Stream, value: Readonly<T>): number | Promise<number>;
  delayError(
    context: MessageContext,
    stream: Stream,
    value: Readonly<T>,
    error: unknown,
    out: Collector<T>
  ): Completion;
}

export interface FlatMapFunction<T, R> {
  flatMap(
    context: MessageContext,
    stream: Stream,
    value: Readonly<T>,
    out: Collector<R>
  ): Completion;
}

export interface KeyByFunction<T, K, V> {
  keyBy(
    context: MessageContext,
    stream: Stream,
    value: Readonly<T>,
    out: Collector<KeyValue<K, V>>
  ): Completion;
}

export interface JoinFunction<K, L, R, O> {
  join(
    context: MessageContext,
    stream: Stream,
    key: K,
    left: readonly Readonly<L>[],
    right: readonly Readonly<R>[],
    out: Collector<O>
  ): boolean | Promise<boolean>;
}

export interface MapFunction<T, R> {
  map(context: MessageContext, stream: Stream, value: Readonly<T>, out: Collector<R>): Completion;
}

export interface MultiJoinFunction<K, T, R> {
  multiJoin(
    context: MessageContext,
    stream: Stream,
    key: K,
    values: readonly [readonly Readonly<T>[], ...(readonly (readonly unknown[])[])],
    out: Collector<R>
  ): boolean | Promise<boolean>;
}

export interface ProcessFunction<T, R, E> {
  process(
    context: MessageContext,
    stream: Stream,
    value: Readonly<T>,
    out: Collector<R>,
    errorOut: Collector<E>
  ): Completion;
}
