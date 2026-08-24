import { type Consumer, type TypedSinkStream, type TypedSinkStreamWithResult } from "../../runtime/index.js";
export declare function makeTemporalSinkEndpointConsumer<T, E>(stream: TypedSinkStream<T, E>): Consumer<T>;
export declare function makeTemporalSinkEndpointConsumerWithResult<T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>): Consumer<T>;
//# sourceMappingURL=temporal.d.ts.map