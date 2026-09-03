import type { Context, MessageContext } from "../../runtime/context.js";
import type { ManagedDataConnector } from "../../runtime/data-connector.js";
import type { RuntimeEnvironment } from "../../runtime/environment/index.js";
import { type EndpointEnvelope, type EndpointResult } from "./contracts.js";
export declare function temporalCronExpression(expression: string): string;
export type TemporalConnectorEndpointHandler = (envelope: EndpointEnvelope, context: MessageContext, cancellationSignal?: AbortSignal) => Promise<EndpointResult>;
export interface TemporalConnectorOptions {
    /** Compiled service-owned module exporting every direct Workflow endpoint. */
    readonly workflowsPath?: string | undefined;
}
/**
 * Workflow interceptor modules shared by live Workers and offline history
 * replayers. Keeping this list in one place prevents replay from silently
 * using different native-header propagation semantics.
 */
export declare function temporalWorkflowInterceptorModules(): string[];
/** Replay one Temporal Event History with the live Worker's Workflow bundle. */
export declare function replayTemporalWorkflowHistory(workflowsPath: string, history: unknown, workflowId?: string): Promise<void>;
export declare class TemporalConnector implements ManagedDataConnector {
    #private;
    readonly id: number;
    readonly name: string;
    constructor(connectorId: number, environment: RuntimeEnvironment, options?: TemporalConnectorOptions);
    registerEndpoint(endpointId: number, handler: TemporalConnectorEndpointHandler): void;
    assertOptions(options: TemporalConnectorOptions): void;
    registerEndpointSubmission(endpointId: number): void;
    start(context: Context): Promise<void>;
    startAdmission(context: Context): Promise<void>;
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
export declare function makeTemporalConnector(connectorId: number, environment: RuntimeEnvironment, options?: TemporalConnectorOptions): TemporalConnector;
export declare function endpointWorkflowId(connectorName: string, endpointName: string, messageId: string): string;
//# sourceMappingURL=connector.d.ts.map