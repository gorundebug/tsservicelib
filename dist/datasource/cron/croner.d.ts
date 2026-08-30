import { Context, InputDataSource, type Consumer, type RuntimeEnvironment, type ScheduleEndpointFunction, type ScheduleTrigger, type TypedInputStream } from "../../runtime/index.js";
export declare class CronDataSource extends InputDataSource {
    #private;
    constructor(connectorId: number, environment: RuntimeEnvironment);
    start(_context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    private cronEndpoints;
}
export declare function makeCronEndpointConsumer<T, R, E>(stream: TypedInputStream<T, R, E>, function_: ScheduleEndpointFunction<T>): Consumer<ScheduleTrigger>;
//# sourceMappingURL=croner.d.ts.map