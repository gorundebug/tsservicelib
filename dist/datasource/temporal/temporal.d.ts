import { type Consumer, type ScheduleTrigger, type TypedInputStream } from "../../runtime/index.js";
export declare function makeTemporalEndpointConsumer<T, R, E>(stream: TypedInputStream<T, R, E>): Consumer<T>;
export declare function makeTemporalScheduleEndpointConsumer<R, E>(stream: TypedInputStream<ScheduleTrigger, R, E>): Consumer<ScheduleTrigger>;
//# sourceMappingURL=temporal.d.ts.map