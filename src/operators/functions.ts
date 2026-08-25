import type { Collector } from "../runtime/collector.js";
import type { MessageContext } from "../runtime/context.js";
import type { KeyValue } from "../runtime/datastruct/key-value.js";
import type { Completion, Stream } from "../runtime/stream.js";

export interface When {
  valueType(): string;
  whenConsumer(): Stream;
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
