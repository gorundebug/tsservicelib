import { Context, type MessageContext } from "../context.js";
import type { RuntimeEnvironment } from "../environment/runtime-environment.js";
import type { JoinStorage, JoinStorageConfig, JoinValueCallback } from "./join-storage.js";
/** Event-loop implementation of Go's two-generation HashMapJoinStorage. */
export declare class HashMapJoinStorage<K> implements JoinStorage<K> {
    #private;
    constructor(environment: RuntimeEnvironment, config: JoinStorageConfig);
    size(): number;
    start(context: Context): void;
    stop(context: Context): Promise<void>;
    joinValue(context: MessageContext, key: K, index: number, value: unknown, callback: JoinValueCallback): Promise<void>;
    private effectiveTTL;
    private expired;
    private findLive;
    private createItem;
    private armDeadline;
    private locate;
    private removeAt;
    private armRotation;
    private rotate;
}
//# sourceMappingURL=hash-map-join-storage.d.ts.map