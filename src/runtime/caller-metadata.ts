import type { Caller } from "./stream.js";

export type CallerType = "taskpool" | "prioritytaskpool" | "parallel" | "durable";

export interface CallerMetadata {
  readonly type: CallerType;
  readonly taskPoolName?: string | undefined;
}

const metadata = new WeakMap<object, CallerMetadata>();

export function setCallerMetadata<T>(caller: Caller<T>, value: CallerMetadata): Caller<T> {
  metadata.set(caller, value);
  return caller;
}

export function callerMetadata<T>(caller: Caller<T>): CallerMetadata | undefined {
  return metadata.get(caller);
}
