import type { Context } from "../context.js";
import type { DurableEnvelope, DurableLinkHandler, DurableLinkId, DurableTransport } from "../durable.js";
import type { RuntimeEnvironment } from "../environment/index.js";
import { type EndpointEnvelope, type EndpointResult } from "./contracts.js";
export type TemporalEndpointHandler = (envelope: EndpointEnvelope, cancellationSignal?: AbortSignal) => Promise<EndpointResult>;
export declare class TemporalConnector implements DurableTransport {
    #private;
    readonly id: number;
    readonly name: string;
    constructor(connectorId: number, environment: RuntimeEnvironment);
    registerLink(link: DurableLinkId, handler: DurableLinkHandler): void;
    registerEndpoint(endpointId: number, handler: TemporalEndpointHandler): void;
    registerEndpointSubmission(endpointId: number): void;
    start(context: Context): Promise<void>;
    stopAdmission(_context: Context): Promise<void>;
    stop(_context: Context): Promise<void>;
    submitLink(link: DurableLinkId, envelope: DurableEnvelope): Promise<void>;
    submitEndpoint(endpointId: number, envelope: EndpointEnvelope, waitForResult: boolean): Promise<EndpointResult>;
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