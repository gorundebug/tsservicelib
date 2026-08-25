import type { Context, MessageContext } from "../../runtime/context.js";
import { DurableCallContext } from "../../runtime/durable-call-context.js";
import type { DurableEnvelope, DurableLinkHandler, DurableLinkId, DurableTransport } from "../../runtime/durable.js";
import type { RuntimeEnvironment } from "../../runtime/environment/index.js";
import { type EndpointEnvelope, type EndpointResult } from "./contracts.js";
export declare function temporalCronExpression(expression: string): string;
export type TemporalEndpointHandler = (envelope: EndpointEnvelope, context: MessageContext, cancellationSignal?: AbortSignal, durableCallContext?: DurableCallContext) => Promise<EndpointResult>;
export declare class TemporalConnector implements DurableTransport {
    #private;
    readonly id: number;
    readonly name: string;
    constructor(connectorId: number, environment: RuntimeEnvironment);
    private continuationActivityType;
    registerLink(link: DurableLinkId, handler: DurableLinkHandler): void;
    private durableDiagnostics;
    registerEndpoint(endpointId: number, handler: TemporalEndpointHandler): void;
    registerEndpointSubmission(endpointId: number): void;
    start(context: Context): Promise<void>;
    stopAdmission(_context: Context): Promise<void>;
    stop(_context: Context): Promise<void>;
    submitLink(context: MessageContext, link: DurableLinkId, envelope: DurableEnvelope): Promise<void>;
    submitEndpoint(context: MessageContext, endpointId: number, envelope: EndpointEnvelope, waitForResult: boolean): Promise<EndpointResult>;
    private connect;
    private queueActivities;
    private shutdownWorkers;
    private ensureSchedule;
    private endpointRegistration;
    private config;
    private linkConfig;
    private endpointConfig;
    private client;
}
export declare function makeTemporalConnector(connectorId: number, environment: RuntimeEnvironment): TemporalConnector;
//# sourceMappingURL=connector.d.ts.map