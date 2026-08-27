import { type Completion, type Consumer, type MessageContext, type ScheduleEndpointFunction, type StreamContext, type TypedInputStream } from "../../runtime/index.js";
export interface TemporalEndpointHandler<State, Input, T, R, E> {
    beginRequest(context: MessageContext, stream: StreamContext<T, R, E>): {
        readonly context: MessageContext;
        readonly state: State;
    } | Promise<{
        readonly context: MessageContext;
        readonly state: State;
    }>;
    consumeMessage(context: MessageContext, stream: StreamContext<T, R, E>, state: State, value: Readonly<Input>): Completion;
    endRequest(context: MessageContext, stream: StreamContext<T, R, E>, error: Error | undefined, state: State): Completion;
}
export declare function makeTemporalEndpointConsumer<T, R, E>(stream: TypedInputStream<T, R, E>): Consumer<T>;
export declare function makeTemporalEndpointConsumerWithHandler<State, T, R, E>(stream: TypedInputStream<T, R, E>, handler: TemporalEndpointHandler<State, T, T, R, E>): Consumer<T>;
export declare function makeTemporalScheduleEndpointConsumer<T, R, E>(stream: TypedInputStream<T, R, E>, function_: ScheduleEndpointFunction<T>): Consumer<T>;
//# sourceMappingURL=temporal.d.ts.map