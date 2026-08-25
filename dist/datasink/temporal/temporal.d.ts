import { type Consumer, type MessageContext, type Stream, type TypedSinkStream, type TypedSinkStreamWithResult } from "../../runtime/index.js";
export interface TemporalEndpointHandler<State, T> {
    beginRequest(context: MessageContext, stream: Stream): Promise<State>;
    getMessageId(context: MessageContext, stream: Stream, state: State, value: T): string;
    endRequest(context: MessageContext, stream: Stream, error: Error | undefined, state: State): Promise<void>;
}
export declare function makeTemporalSinkEndpointConsumer<T, E>(stream: TypedSinkStream<T, E>): Consumer<T>;
export declare function makeTemporalSinkEndpointConsumerWithHandler<State, T, E>(stream: TypedSinkStream<T, E>, handler: TemporalEndpointHandler<State, T>): Consumer<T>;
export declare function makeTemporalSinkEndpointConsumerWithResult<T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>): Consumer<T>;
export declare function makeTemporalSinkEndpointConsumerWithResultHandler<State, T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>, handler: TemporalEndpointHandler<State, T>): Consumer<T>;
//# sourceMappingURL=temporal.d.ts.map