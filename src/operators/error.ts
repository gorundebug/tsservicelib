import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { StreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { RuntimeEnvironment } from "../runtime/environment/runtime-environment.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import type { Completion, Stream, TypedStreamConsumer } from "../runtime/stream.js";

export class ErrorStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
  readonly #recordOwnerCall: (context: MessageContext) => void;

  public constructor(
    config: StreamConfig,
    environment: RuntimeEnvironment,
    serde: StreamSerde<T>,
    owner: Stream
  ) {
    super(config, environment, serde);
    environment.registerStream(this);
    this.#recordOwnerCall = environment.makeLinkRecorder(owner, this);
  }

  /** Mirrors Go ErrorStream.GetID without changing the configured stream ID. */
  public override get id(): number {
    return -super.id;
  }

  public consume(context: MessageContext, value: T): Completion {
    return this.out(context, value);
  }

  public override out(context: MessageContext, value: T): Completion {
    this.#recordOwnerCall(context);
    return this.emit(context, value);
  }

  public functionImplementation(): undefined {
    return undefined;
  }
}
