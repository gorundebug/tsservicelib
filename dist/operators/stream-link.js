/**
 * Package-internal equivalent of Go's streamLink. Internal graph edges keep
 * the identity and live configuration of the operator that owns them.
 */
export class StreamLink {
    #stream;
    constructor(stream) {
        this.#stream = stream;
    }
    get id() {
        return this.#stream.id;
    }
    get name() {
        return this.#stream.name;
    }
    get transformationName() {
        return this.#stream.transformationName;
    }
    runtimeEnvironment() {
        return this.#stream.runtimeEnvironment();
    }
    config() {
        return this.#stream.config();
    }
}
//# sourceMappingURL=stream-link.js.map