import type { Completion } from "../../runtime/stream.js";
import type { TypedInputStream } from "../../runtime/data-source.js";
import type { MessageContext } from "../../runtime/context.js";
import type { EndpointWireEnvelope, EndpointWireResult, EndpointWorkflowRequest } from "./contracts.js";
import type { TemporalWorkflowEnvironment } from "./workflow-environment.js";
export declare function servicelibTemporalEndpointV1(request: EndpointWorkflowRequest): Promise<EndpointWireResult>;
export { servicelibTemporalEndpointV1 as "servicelib.temporal-endpoint.v1" };
export interface TemporalWorkflowEndpoint<T, R, E> {
    readonly request: EndpointWorkflowRequest;
    readonly environment: TemporalWorkflowEnvironment;
    readonly stream: TypedInputStream<T, R, E>;
    activate(context: MessageContext, envelope: EndpointWireEnvelope): Completion;
}
/** Execute one generated service-owned Workflow endpoint in the Workflow isolate. */
export declare function executeTemporalWorkflowEndpoint<T, R, E>(endpoint: TemporalWorkflowEndpoint<T, R, E>): Promise<EndpointWireResult>;
export declare function scheduledEnvelope(envelope: EndpointWireEnvelope): EndpointWireEnvelope;
//# sourceMappingURL=workflows.d.ts.map