import { ConsumedStream } from "../runtime/consumed-stream.js";
import { JoinType, type JoinStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { KeyValue } from "../runtime/datastruct/key-value.js";
import {
  makeJoinStorage,
  type JoinStorage,
  type JoinStorageConfig,
  type JoinValues
} from "../runtime/store/join-storage.js";
import type { Completion, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { JoinFunction } from "./functions.js";
import { StreamLink } from "./stream-link.js";

class JoinStorageConfigView implements JoinStorageConfig {
  readonly #stream: ConsumedStream<unknown>;

  public constructor(stream: ConsumedStream<unknown>) {
    this.#stream = stream;
  }

  public ttlMs(): number {
    return (this.#stream.config() as JoinStreamConfig).ttl;
  }

  public renewTTL(): boolean {
    return (this.#stream.config() as JoinStreamConfig).renewTTL;
  }

  public name(): string {
    return this.#stream.name;
  }
}

class JoinLink<K, L, R, O> extends StreamLink implements TypedStreamConsumer<KeyValue<K, R>> {
  readonly #join: JoinStream<K, L, R, O>;

  public constructor(join: JoinStream<K, L, R, O>) {
    super(join);
    this.#join = join;
  }

  public consume(context: MessageContext, value: KeyValue<K, R>): Completion {
    return this.#join.consumeRight(context, value);
  }
}

export class JoinStream<K, L, R, O>
  extends ConsumedStream<O>
  implements TypedStreamConsumer<KeyValue<K, L>>
{
  readonly #function: JoinFunction<K, L, R, O>;
  readonly #storage: JoinStorage<K>;

  public constructor(
    config: JoinStreamConfig,
    left: TypedStream<KeyValue<K, L>>,
    right: TypedStream<KeyValue<K, R>>,
    function_: JoinFunction<K, L, R, O>
  ) {
    const environment = left.runtimeEnvironment();
    if (right.runtimeEnvironment() !== environment) {
      throw new Error(`join stream ${config.name} sources belong to different environments`);
    }
    super(config, environment, environment.serdeByName<O>(config.valueType));
    this.#function = function_;
    const storageConfig = new JoinStorageConfigView(this);
    const customStorage = environment.createKeyValueJoinStorage<K>(
      config.joinStorage,
      storageConfig,
      this
    );
    if (customStorage !== undefined) {
      this.#storage = customStorage;
    } else {
      this.#storage = makeJoinStorage(config.joinStorage, environment, storageConfig);
    }
    environment.registerStorage(this.#storage);
    left.setConsumer(this);
    environment.registerStream(this);
    right.setConsumer(new JoinLink(this));
  }

  public functionImplementation(): JoinFunction<K, L, R, O> {
    return this.#function;
  }

  public storage(): JoinStorage<K> {
    return this.#storage;
  }

  public consume(context: MessageContext, value: KeyValue<K, L>): Promise<void> {
    if (!this.tracingEnabled(context)) {
      return this.consumeValue(context, value.key, 0, value.value);
    }
    return Promise.resolve(
      this.traceCompletion(context, "stream.join", (spanContext) =>
        this.consumeValue(spanContext, value.key, 0, value.value)
      )
    );
  }

  public consumeRight(context: MessageContext, value: KeyValue<K, R>): Promise<void> {
    if (!this.tracingEnabled(context)) {
      return this.consumeValue(context, value.key, 1, value.value);
    }
    return Promise.resolve(
      this.traceCompletion(context, "stream.join", (spanContext) =>
        this.consumeValue(spanContext, value.key, 1, value.value)
      )
    );
  }

  private consumeValue(
    context: MessageContext,
    key: K,
    index: number,
    value: unknown
  ): Promise<void> {
    return this.#storage.joinValue(context, key, index, value, (values) =>
      this.callFunction(context, key, values)
    );
  }

  private callFunction(
    context: MessageContext,
    key: K,
    values: JoinValues
  ): boolean | Promise<boolean> {
    const joinType = (this.config() as JoinStreamConfig).joinType;
    const left = (values[0] ?? []) as L[];
    const right = (values[1] ?? []) as R[];
    const canCall =
      (joinType === JoinType.Inner && left.length > 0 && right.length > 0) ||
      (joinType === JoinType.Left && left.length > 0) ||
      (joinType === JoinType.Right && right.length > 0) ||
      joinType === JoinType.Outer;
    return canCall ? this.#function.join(context, this, key, left, right, this) : false;
  }
}

export function makeJoinStream<K, L, R, O>(
  config: JoinStreamConfig,
  left: TypedStream<KeyValue<K, L>>,
  right: TypedStream<KeyValue<K, R>>,
  function_: JoinFunction<K, L, R, O>
): JoinStream<K, L, R, O> {
  return new JoinStream(config, left, right, function_);
}
