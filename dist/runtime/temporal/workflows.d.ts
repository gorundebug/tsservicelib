import type { DurableWorkflowRequest, EndpointWireResult, EndpointWorkflowRequest } from "./contracts.js";
export declare function servicegenDurableLinkV1(request: DurableWorkflowRequest): Promise<void>;
export declare function servicegenTemporalEndpointV1(request: EndpointWorkflowRequest): Promise<EndpointWireResult>;
export { servicegenDurableLinkV1 as "servicegen.durable-link.v1", servicegenTemporalEndpointV1 as "servicegen.temporal-endpoint.v1" };
//# sourceMappingURL=workflows.d.ts.map