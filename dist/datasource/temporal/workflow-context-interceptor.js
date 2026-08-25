import {} from "@temporalio/workflow";
export function interceptors() {
    let carrier = {};
    const inbound = {
        execute(input, next) {
            carrier = input.headers;
            return next(input);
        }
    };
    const outbound = {
        scheduleActivity(input, next) {
            return next({ ...input, headers: { ...carrier, ...input.headers } });
        }
    };
    return { inbound: [inbound], outbound: [outbound] };
}
//# sourceMappingURL=workflow-context-interceptor.js.map