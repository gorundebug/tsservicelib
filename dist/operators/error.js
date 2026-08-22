import { ConsumedStream } from "../runtime/index.js";
export class ErrorStream extends ConsumedStream {
    #recordOwnerCall;
    constructor(config, environment, serde, owner) {
        super(config, environment, serde);
        environment.registerStream(this);
        this.#recordOwnerCall = environment.makeLinkRecorder(owner, this);
    }
    /** Mirrors Go ErrorStream.GetID without changing the configured stream ID. */
    get id() {
        return -super.id;
    }
    consume(context, value) {
        return this.out(context, value);
    }
    out(context, value) {
        this.#recordOwnerCall(context);
        return this.emit(context, value);
    }
    functionImplementation() {
        return undefined;
    }
}
//# sourceMappingURL=error.js.map