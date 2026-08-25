import type { DurableWorkflowRequest, EndpointWireResult, EndpointWorkflowRequest } from "./contracts.js";
export declare function servicelibDurableLinkV1(request: DurableWorkflowRequest): Promise<void>;
export declare function servicelibTemporalEndpointV1(request: EndpointWorkflowRequest): Promise<EndpointWireResult>;
export { servicelibDurableLinkV1 as "servicelib.durable-link.v1", servicelibTemporalEndpointV1 as "servicelib.temporal-endpoint.v1" };
//# sourceMappingURL=workflows.d.ts.map