import { Context, InputDataSource, type Consumer, type RuntimeEnvironment, type ScheduleTrigger, type TypedInputStream } from "../../runtime/index.js";
export declare class CronDataSource extends InputDataSource {
    #private;
    constructor(connectorId: number, environment: RuntimeEnvironment);
    start(_context: Context): Promise<void>;
    stop(_context: Context): Promise<void>;
    private cronEndpoints;
}
export declare function makeCronEndpointConsumer<R, E>(stream: TypedInputStream<ScheduleTrigger, R, E>): Consumer<ScheduleTrigger>;
//# sourceMappingURL=croner.d.ts.map