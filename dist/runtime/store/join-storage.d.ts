import type { MessageContext } from "../context.js";
import { JoinStorageType } from "../config/index.js";
import type { RuntimeEnvironment } from "../environment/runtime-environment.js";
import type { Storage } from "./storage.js";
export type JoinValues = unknown[][];
export type JoinValueCallback = (values: JoinValues) => boolean | Promise<boolean>;
export interface JoinStorageConfig {
    ttlMs(): number;
    renewTTL(): boolean;
    name(): string;
}
export interface JoinStorage<K> extends Storage {
    joinValue(context: MessageContext, key: K, index: number, value: unknown, callback: JoinValueCallback): Promise<void>;
    size(): number;
}
export declare function makeJoinStorage<K>(storageType: JoinStorageType, environment: RuntimeEnvironment, config: JoinStorageConfig): JoinStorage<K>;
//# sourceMappingURL=join-storage.d.ts.map