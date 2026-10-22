/** Counts a graph link without loading any tracing runtime. */
export class CountedCaller {
    caller;
    recordCall;
    constructor(caller, recordCall) {
        this.caller = caller;
        this.recordCall = recordCall;
    }
    isAsync() {
        return this.caller.isAsync();
    }
    consume(context, value) {
        this.recordCall(context);
        return this.caller.consume(context, value);
    }
}
//# sourceMappingURL=counted-caller.js.map