import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { CaseStreamConfig, WhenStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type {
  RuntimeBuildable,
  RuntimeEnvironment
} from "../runtime/environment/runtime-environment.js";
import type { Completion, Stream, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { BuildSwitchFunction, When } from "./functions.js";

interface WhenBranch<T> extends Stream, When {
  consumeCase(context: MessageContext, value: T): Completion;
}

export class WhenStream<T, R>
  extends ConsumedStream<R>
  implements WhenBranch<T>, TypedStreamConsumer<R>
{
  readonly #caseStream: CaseStream<T>;
  readonly #index: number;
  readonly #valueType: string;

  public constructor(config: WhenStreamConfig, caseStream: CaseStream<T>, index: number) {
    super(
      config,
      caseStream.runtimeEnvironment(),
      caseStream.runtimeEnvironment().serdeByName<R>(config.valueType)
    );
    this.#caseStream = caseStream;
    this.#index = index;
    this.#valueType = config.valueType;
    caseStream.runtimeEnvironment().registerStream(this);
  }

  public override get name(): string {
    return super.name.length > 0
      ? super.name
      : `${this.#caseStream.name}CaseLink${String(this.#index)}`;
  }

  public valueType(): string {
    return this.#valueType;
  }

  public whenConsumer(): Stream {
    return this.consumer() ?? this;
  }

  public consume(context: MessageContext, value: R): Completion {
    return this.emit(context, value);
  }

  public consumeCase(context: MessageContext, value: T): Completion {
    return this.consumeValidated(context, value);
  }

  public functionImplementation(): undefined {
    return undefined;
  }

  private consumeValidated(context: MessageContext, value: unknown): Completion {
    const environment: RuntimeEnvironment = this.runtimeEnvironment();
    environment.assertSerdeValue<R>(this.#valueType, value);
    return this.consume(context, value);
  }
}

export class CaseStream<T>
  extends ConsumedStream<T>
  implements TypedStreamConsumer<T>, RuntimeBuildable
{
  readonly #buildSwitch: BuildSwitchFunction<T>;
  readonly #whenStreams: WhenBranch<T>[] = [];
  #selector: ((value: T) => number) | undefined;

  public constructor(
    config: CaseStreamConfig,
    source: TypedStream<T>,
    buildSwitch: BuildSwitchFunction<T>
  ) {
    const environment = source.runtimeEnvironment();
    super(config, environment, source.serde());
    this.#buildSwitch = buildSwitch;
    environment.registerStream(this);
    source.setConsumer(this);
    environment.registerRuntimeBuildable(this);
  }

  public addStream<R>(config: WhenStreamConfig): WhenStream<T, R> {
    const stream = new WhenStream<T, R>(config, this, this.#whenStreams.length);
    this.#whenStreams.push(stream);
    return stream;
  }

  public build(): void {
    this.#selector = this.#buildSwitch.buildSwitch(this, this.#whenStreams);
  }

  public override consumers(): readonly Stream[] {
    return this.#whenStreams;
  }

  public consume(context: MessageContext, value: T): Completion {
    if (!this.tracingEnabled(context)) {
      return this.consumeCase(context, value);
    }
    return this.traceCompletion(context, "stream.case", (spanContext) =>
      this.consumeCase(spanContext, value)
    );
  }

  public functionImplementation(): BuildSwitchFunction<T> {
    return this.#buildSwitch;
  }

  private consumeCase(context: MessageContext, value: T): Completion {
    if (this.#selector === undefined) {
      throw new Error(`CaseStream ${this.name} is not built`);
    }
    const index = this.#selector(value);
    const branch = this.#whenStreams[index];
    if (branch === undefined) {
      throw new RangeError(
        `case selector returned branch ${String(index)}, but only ${String(this.#whenStreams.length)} branches exist`
      );
    }
    return branch.consumeCase(context, value);
  }
}

export function makeCaseStream<T>(
  config: CaseStreamConfig,
  source: TypedStream<T>,
  buildSwitch: BuildSwitchFunction<T>
): CaseStream<T> {
  return new CaseStream(config, source, buildSwitch);
}

export function makeWhenStream<T, R>(
  config: WhenStreamConfig,
  caseStream: CaseStream<T>
): WhenStream<T, R> {
  return caseStream.addStream(config);
}
