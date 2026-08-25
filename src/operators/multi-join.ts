import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { MultiJoinStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { KeyValue } from "../runtime/datastruct/key-value.js";
import {
  makeJoinStorage,
  type JoinStorage,
  type JoinStorageConfig
} from "../runtime/store/join-storage.js";
import type { Completion, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { MultiJoinFunction } from "./functions.js";
import { StreamLink } from "./stream-link.js";

const bindMultiJoinRight = Symbol("bindMultiJoinRight");

class MultiJoinStorageConfigView implements JoinStorageConfig {
  readonly #stream: ConsumedStream<unknown>;

  public constructor(stream: ConsumedStream<unknown>) {
    this.#stream = stream;
  }

  public ttlMs(): number {
    return (this.#stream.config() as MultiJoinStreamConfig).ttl;
  }

  public renewTTL(): boolean {
    return (this.#stream.config() as MultiJoinStreamConfig).renewTTL;
  }

  public name(): string {
    return this.#stream.name;
  }
}

class MultiJoinLink<K, T, R, V> extends StreamLink implements TypedStreamConsumer<KeyValue<K, V>> {
  readonly #multiJoin: MultiJoinStream<K, T, R>;
  readonly #index: number;

  public constructor(multiJoin: MultiJoinStream<K, T, R>, index: number) {
    super(multiJoin);
    this.#multiJoin = multiJoin;
    this.#index = index;
  }

  public consume(context: MessageContext, value: KeyValue<K, V>): Completion {
    return this.#multiJoin.consumeRight(context, this.#index, value);
  }
}

export class MultiJoinStream<K, T, R>
  extends ConsumedStream<R>
  implements TypedStreamConsumer<KeyValue<K, T>>
{
  readonly #function: MultiJoinFunction<K, T, R>;
  readonly #storage: JoinStorage<K>;
  #linkCount = 0;

  public constructor(
    config: MultiJoinStreamConfig,
    left: TypedStream<KeyValue<K, T>>,
    function_: MultiJoinFunction<K, T, R>
  ) {
    const environment = left.runtimeEnvironment();
    super(config, environment, environment.serdeByName<R>(config.valueType));
    this.#function = function_;
    const storageConfig = new MultiJoinStorageConfigView(this);
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
  }

  public functionImplementation(): MultiJoinFunction<K, T, R> {
    return this.#function;
  }

  public storage(): JoinStorage<K> {
    return this.#storage;
  }

  public consume(context: MessageContext, value: KeyValue<K, T>): Promise<void> {
    if (!this.tracingEnabled(context)) {
      return this.consumeValue(context, value.key, 0, value.value);
    }
    return Promise.resolve(
      this.traceCompletion(context, "stream.join", (spanContext) =>
        this.consumeValue(spanContext, value.key, 0, value.value)
      )
    );
  }

  public consumeRight<V>(
    context: MessageContext,
    index: number,
    value: KeyValue<K, V>
  ): Promise<void> {
    if (!this.tracingEnabled(context)) {
      return this.consumeValue(context, value.key, index, value.value);
    }
    return Promise.resolve(
      this.traceCompletion(context, "stream.join", (spanContext) =>
        this.consumeValue(spanContext, value.key, index, value.value)
      )
    );
  }

  public [bindMultiJoinRight]<V>(right: TypedStream<KeyValue<K, V>>): void {
    if (right.runtimeEnvironment() !== this.runtimeEnvironment()) {
      throw new Error(`multi-join stream ${this.name} sources belong to different environments`);
    }
    const index = this.#linkCount + 1;
    right.setConsumer(new MultiJoinLink(this, index));
    this.#linkCount = index;
  }

  private consumeValue(
    context: MessageContext,
    key: K,
    index: number,
    value: unknown
  ): Promise<void> {
    return this.#storage.joinValue(context, key, index, value, (values) => {
      if ((values[0]?.length ?? 0) === 0) {
        return false;
      }
      // Slot zero is populated exclusively by consume(KeyValue<K, T>); right
      // links are assigned indices starting at one. The assertion restores
      // that construction-time invariant after heterogeneous storage erasure.
      return this.#function.multiJoin(context, this, key, values as [T[], ...unknown[][]], this);
    });
  }
}

export function makeMultiJoinStream<K, T, R>(
  config: MultiJoinStreamConfig,
  left: TypedStream<KeyValue<K, T>>,
  function_: MultiJoinFunction<K, T, R>
): MultiJoinStream<K, T, R> {
  return new MultiJoinStream(config, left, function_);
}

export function makeMultiJoinLink<K, T, V, R>(
  multiJoin: MultiJoinStream<K, T, R>,
  right: TypedStream<KeyValue<K, V>>
): void {
  multiJoin[bindMultiJoinRight](right);
}
