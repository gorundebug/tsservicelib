import type { Consumer } from "../../runtime/stream.js";
import type { TypedSinkStream, TypedSinkStreamWithResult } from "../../runtime/data-sink.js";
/** Attach a plain Temporal sink to a graph executing inside a Workflow isolate. */
export declare function makeTemporalWorkflowSinkEndpointConsumer<T, E>(stream: TypedSinkStream<T, E>): Consumer<T>;
/** Attach a result-producing Temporal sink to a Workflow graph. */
export declare function makeTemporalWorkflowSinkEndpointConsumerWithResult<T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>): Consumer<T>;
//# sourceMappingURL=workflow-sink.d.ts.map