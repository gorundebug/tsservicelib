import type { Context, MessageContext } from "../../context.js";
export declare const SpanStatusCode: {
    readonly Unset: "unset";
    readonly Ok: "ok";
    readonly Error: "error";
};
export type SpanStatusCode = (typeof SpanStatusCode)[keyof typeof SpanStatusCode];
export type Attribute = {
    readonly key: string;
    readonly type: "string";
    readonly value: string;
} | {
    readonly key: string;
    readonly type: "int64";
    readonly value: bigint;
} | {
    readonly key: string;
    readonly type: "float64";
    readonly value: number;
} | {
    readonly key: string;
    readonly type: "bool";
    readonly value: boolean;
};
export interface SpanContext {
    readonly traceId: string;
    readonly spanId: string;
    readonly isValid: boolean;
}
export interface Span {
    end(): void;
    setAttributes(attributes: readonly Attribute[]): void;
    recordError(error: Error): void;
    setStatus(code: SpanStatusCode, description: string): void;
    addEvent(name: string, attributes?: readonly Attribute[]): void;
    spanContext(): SpanContext;
}
export interface StartedSpan {
    readonly context: MessageContext;
    readonly span: Span;
}
export interface Tracer {
    start(context: MessageContext, spanName: string, attributes?: readonly Attribute[]): StartedSpan;
}
export interface Tracing {
    enabled(): boolean;
    tracer(name: string): Tracer;
}
export interface TracingEngine {
    tracing(): Tracing;
    shutdown(context: Context): Promise<void>;
}
export declare function spanError(span: Span | undefined, error: Error): void;
export declare function stringAttribute(key: string, value: string): Attribute;
export declare function int64Attribute(key: string, value: bigint): Attribute;
export declare function float64Attribute(key: string, value: number): Attribute;
export declare function boolAttribute(key: string, value: boolean): Attribute;
//# sourceMappingURL=tracing.d.ts.map