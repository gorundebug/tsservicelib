import type { Context, MessageContext } from "../../runtime/context.js";
import type { ManagedDataConnector } from "../../runtime/data-connector.js";
import type { RuntimeEnvironment } from "../../runtime/environment/index.js";
import { type EndpointEnvelope, type EndpointResult } from "./contracts.js";
export declare function temporalCronExpression(expression: string): string;
export type TemporalEndpointHandler = (envelope: EndpointEnvelope, context: MessageContext, cancellationSignal?: AbortSignal) => Promise<EndpointResult>;
export declare class TemporalConnector implements ManagedDataConnector {
    #private;
    readonly id: number;
    readonly name: string;
    constructor(connectorId: number, environment: RuntimeEnvironment);
    registerEndpoint(endpointId: number, handler: TemporalEndpointHandler): void;
    registerEndpointSubmission(endpointId: number): void;
    start(context: Context): Promise<void>;
    stopAdmission(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    submitEndpoint(context: MessageContext, endpointId: number, envelope: EndpointEnvelope, waitForResult: boolean): Promise<EndpointResult>;
    private queueActivities;
    private activityDiagnostics;
    private ensureSchedule;
    private endpointRegistration;
    private config;
    private endpointConfig;
    private client;
    private shutdownWorkers;
}
export declare function makeTemporalConnector(connectorId: number, environment: RuntimeEnvironment): TemporalConnector;
export declare function endpointWorkflowId(connectorName: string, endpointName: string, messageId: string): string;
//# sourceMappingURL=connector.d.ts.map