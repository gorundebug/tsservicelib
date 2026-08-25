import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { CaseStreamConfig, WhenStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { RuntimeBuildable } from "../runtime/environment/runtime-environment.js";
import type { Completion, Stream, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { BuildSwitchFunction, When } from "./functions.js";
interface WhenBranch<T> extends Stream, When {
    consumeCase(context: MessageContext, value: T): Completion;
}
export declare class WhenStream<T, R> extends ConsumedStream<R> implements WhenBranch<T>, TypedStreamConsumer<R> {
    #private;
    constructor(config: WhenStreamConfig, caseStream: CaseStream<T>, index: number);
    get name(): string;
    valueType(): string;
    whenConsumer(): Stream;
    consume(context: MessageContext, value: R): Completion;
    consumeCase(context: MessageContext, value: T): Completion;
    functionImplementation(): undefined;
    private consumeValidated;
}
export declare class CaseStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T>, RuntimeBuildable {
    #private;
    constructor(config: CaseStreamConfig, source: TypedStream<T>, buildSwitch: BuildSwitchFunction<T>);
    addStream<R>(config: WhenStreamConfig): WhenStream<T, R>;
    build(): void;
    consumers(): readonly Stream[];
    consume(context: MessageContext, value: T): Completion;
    functionImplementation(): BuildSwitchFunction<T>;
    private consumeCase;
}
export declare function makeCaseStream<T>(config: CaseStreamConfig, source: TypedStream<T>, buildSwitch: BuildSwitchFunction<T>): CaseStream<T>;
export declare function makeWhenStream<T, R>(config: WhenStreamConfig, caseStream: CaseStream<T>): WhenStream<T, R>;
export {};
//# sourceMappingURL=case.d.ts.map