import type { RuntimeEnvironment, Stream, StreamConfig } from "../runtime/index.js";
/**
 * Package-internal equivalent of Go's streamLink. Internal graph edges keep
 * the identity and live configuration of the operator that owns them.
 */
export declare abstract class StreamLink implements Stream {
    #private;
    protected constructor(stream: Stream);
    get id(): number;
    get name(): string;
    get transformationName(): string;
    runtimeEnvironment(): RuntimeEnvironment;
    config(): StreamConfig;
}
//# sourceMappingURL=stream-link.d.ts.map