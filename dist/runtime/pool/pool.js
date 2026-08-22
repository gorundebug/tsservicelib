export class PoolStoppedError extends Error {
    constructor(name) {
        super(`pool ${name} is stopped`);
        this.name = "PoolStoppedError";
    }
}
//# sourceMappingURL=pool.js.map