import { type Consumer, type ScheduleEndpointFunction, type ScheduleTrigger, type TypedInputStream } from "../../runtime/index.js";
export declare function makeTemporalEndpointConsumer<T, R, E>(stream: TypedInputStream<T, R, E>): Consumer<T>;
export declare function makeTemporalScheduleEndpointConsumer<T, R, E>(stream: TypedInputStream<T, R, E>, function_: ScheduleEndpointFunction<T>): Consumer<ScheduleTrigger>;
//# sourceMappingURL=temporal.d.ts.map