import { ConsumedStream } from "../runtime/consumed-stream.js";
import { makeJoinStorage } from "../runtime/store/join-storage.js";
import { StreamLink } from "./stream-link.js";
const bindMultiJoinRight = Symbol("bindMultiJoinRight");
class MultiJoinStorageConfigView {
    #stream;
    constructor(stream) {
        this.#stream = stream;
    }
    ttlMs() {
        return this.#stream.config().ttl;
    }
    renewTTL() {
        return this.#stream.config().renewTTL;
    }
    name() {
        return this.#stream.name;
    }
}
class MultiJoinLink extends StreamLink {
    #multiJoin;
    #index;
    constructor(multiJoin, index) {
        super(multiJoin);
        this.#multiJoin = multiJoin;
        this.#index = index;
    }
    consume(context, value) {
        return this.#multiJoin.consumeRight(context, this.#index, value);
    }
}
export class MultiJoinStream extends ConsumedStream {
    #function;
    #storage;
    #linkCount = 0;
    constructor(config, left, function_) {
        const environment = left.runtimeEnvironment();
        super(config, environment, environment.serdeByName(config.valueType));
        this.#function = function_;
        const storageConfig = new MultiJoinStorageConfigView(this);
        const customStorage = environment.createKeyValueJoinStorage(config.joinStorage, storageConfig, this);
        if (customStorage !== undefined) {
            this.#storage = customStorage;
        }
        else {
            this.#storage = makeJoinStorage(config.joinStorage, environment, storageConfig);
        }
        environment.registerStorage(this.#storage);
        left.setConsumer(this);
        environment.registerStream(this);
    }
    functionImplementation() {
        return this.#function;
    }
    storage() {
        return this.#storage;
    }
    consume(context, value) {
        if (!this.tracingEnabled(context)) {
            return this.consumeValue(context, value.key, 0, value.value);
        }
        return Promise.resolve(this.traceCompletion(context, "stream.join", (spanContext) => this.consumeValue(spanContext, value.key, 0, value.value)));
    }
    consumeRight(context, index, value) {
        if (!this.tracingEnabled(context)) {
            return this.consumeValue(context, value.key, index, value.value);
        }
        return Promise.resolve(this.traceCompletion(context, "stream.join", (spanContext) => this.consumeValue(spanContext, value.key, index, value.value)));
    }
    [bindMultiJoinRight](right) {
        if (right.runtimeEnvironment() !== this.runtimeEnvironment()) {
            throw new Error(`multi-join stream ${this.name} sources belong to different environments`);
        }
        const index = this.#linkCount + 1;
        right.setConsumer(new MultiJoinLink(this, index));
        this.#linkCount = index;
    }
    consumeValue(context, key, index, value) {
        return this.#storage.joinValue(context, key, index, value, (values) => {
            if ((values[0]?.length ?? 0) === 0) {
                return false;
            }
            // Slot zero is populated exclusively by consume(KeyValue<K, T>); right
            // links are assigned indices starting at one. The assertion restores
            // that construction-time invariant after heterogeneous storage erasure.
            return this.#function.multiJoin(context, this, key, values, this);
        });
    }
}
export function makeMultiJoinStream(config, left, function_) {
    return new MultiJoinStream(config, left, function_);
}
export function makeMultiJoinLink(multiJoin, right) {
    multiJoin[bindMultiJoinRight](right);
}
//# sourceMappingURL=multi-join.js.map