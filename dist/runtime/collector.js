export class FunctionCollector {
    #collect;
    constructor(collect) {
        this.#collect = collect;
    }
    out(context, value) {
        return this.#collect(context, value);
    }
}
export class CallerCollector {
    #caller;
    constructor(caller) {
        this.#caller = caller;
    }
    out(context, value) {
        return this.#caller?.consume(context, value);
    }
}
export function makeCollector(caller) {
    return new CallerCollector(caller);
}
//# sourceMappingURL=collector.js.map