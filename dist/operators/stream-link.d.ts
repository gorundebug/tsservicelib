import type { StreamConfig } from "../runtime/config/types.js";
import type { RuntimeEnvironment } from "../runtime/environment/runtime-environment.js";
import type { Stream } from "../runtime/stream.js";
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