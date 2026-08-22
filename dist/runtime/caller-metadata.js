const metadata = new WeakMap();
export function setCallerMetadata(caller, value) {
    metadata.set(caller, value);
    return caller;
}
export function callerMetadata(caller) {
    return metadata.get(caller);
}
//# sourceMappingURL=caller-metadata.js.map