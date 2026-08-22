import type { MessageContext } from "../context.js";
import { JoinStorageType } from "../config/index.js";
import type { RuntimeEnvironment } from "../environment/runtime-environment.js";
import { HashMapJoinStorage } from "./hash-map-join-storage.js";
import type { Storage } from "./storage.js";

export type JoinValues = unknown[][];
export type JoinValueCallback = (values: JoinValues) => boolean | Promise<boolean>;

export interface JoinStorageConfig {
  ttlMs(): number;
  renewTTL(): boolean;
  name(): string;
}

export interface JoinStorage<K> extends Storage {
  joinValue(
    context: MessageContext,
    key: K,
    index: number,
    value: unknown,
    callback: JoinValueCallback
  ): Promise<void>;
  size(): number;
}

export function makeJoinStorage<K>(
  storageType: JoinStorageType,
  environment: RuntimeEnvironment,
  config: JoinStorageConfig
): JoinStorage<K> {
  switch (storageType) {
    case JoinStorageType.HashMap:
      return new HashMapJoinStorage<K>(environment, config);
    default:
      throw new Error(`join storage type ${String(storageType)} is not supported`);
  }
}
