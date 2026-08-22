export class StoreAlreadyStartedError extends Error {
    constructor() {
        super("store already started");
        this.name = "StoreAlreadyStartedError";
    }
}
export class StoreNotStartedError extends Error {
    constructor() {
        super("store not started");
        this.name = "StoreNotStartedError";
    }
}
export class StoreStoppedError extends Error {
    constructor() {
        super("store stopped");
        this.name = "StoreStoppedError";
    }
}
export class DuplicateKeyError extends Error {
    constructor(key) {
        super(`duplicate key ${String(key)}`);
        this.name = "DuplicateKeyError";
    }
}
//# sourceMappingURL=storage.js.map