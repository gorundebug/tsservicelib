import { ConsumedStream, JoinType, makeJoinStorage } from "../runtime/index.js";
import { StreamLink } from "./stream-link.js";
class JoinStorageConfigView {
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
class JoinLink extends StreamLink {
    #join;
    constructor(join) {
        super(join);
        this.#join = join;
    }
    consume(context, value) {
        return this.#join.consumeRight(context, value);
    }
}
export class JoinStream extends ConsumedStream {
    #function;
    #storage;
    constructor(config, left, right, function_) {
        const environment = left.runtimeEnvironment();
        if (right.runtimeEnvironment() !== environment) {
            throw new Error(`join stream ${config.name} sources belong to different environments`);
        }
        super(config, environment, environment.serdeByName(config.valueType));
        this.#function = function_;
        const storageConfig = new JoinStorageConfigView(this);
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
        right.setConsumer(new JoinLink(this));
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
    consumeRight(context, value) {
        if (!this.tracingEnabled(context)) {
            return this.consumeValue(context, value.key, 1, value.value);
        }
        return Promise.resolve(this.traceCompletion(context, "stream.join", (spanContext) => this.consumeValue(spanContext, value.key, 1, value.value)));
    }
    consumeValue(context, key, index, value) {
        return this.#storage.joinValue(context, key, index, value, (values) => this.callFunction(context, key, values));
    }
    callFunction(context, key, values) {
        const joinType = this.config().joinType;
        const left = (values[0] ?? []);
        const right = (values[1] ?? []);
        const canCall = (joinType === JoinType.Inner && left.length > 0 && right.length > 0) ||
            (joinType === JoinType.Left && left.length > 0) ||
            (joinType === JoinType.Right && right.length > 0) ||
            joinType === JoinType.Outer;
        return canCall ? this.#function.join(context, this, key, left, right, this) : false;
    }
}
export function makeJoinStream(config, left, right, function_) {
    return new JoinStream(config, left, right, function_);
}
//# sourceMappingURL=join.js.map