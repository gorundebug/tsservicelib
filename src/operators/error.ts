import {
  ConsumedStream,
  type Completion,
  type MessageContext,
  type StreamConfig,
  type Stream,
  type RuntimeEnvironment,
  type StreamSerde,
  type TypedStreamConsumer
} from "../runtime/index.js";

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
