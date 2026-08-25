import type { StreamConfig } from "../runtime/config/types.js";
import type { RuntimeEnvironment } from "../runtime/environment/runtime-environment.js";
import type { Stream } from "../runtime/stream.js";

/**
 * Package-internal equivalent of Go's streamLink. Internal graph edges keep
 * the identity and live configuration of the operator that owns them.
 */
export abstract class StreamLink implements Stream {
  readonly #stream: Stream;

  protected constructor(stream: Stream) {
    this.#stream = stream;
  }

  public get id(): number {
    return this.#stream.id;
  }

  public get name(): string {
    return this.#stream.name;
  }

  public get transformationName(): string {
    return this.#stream.transformationName;
  }

  public runtimeEnvironment(): RuntimeEnvironment {
    return this.#stream.runtimeEnvironment();
  }

  public config(): StreamConfig {
    return this.#stream.config();
  }
}
