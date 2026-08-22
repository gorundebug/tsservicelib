import type { StreamSerde } from "./serde.js";
declare const registerSerde: unique symbol;
declare const resolveSerde: unique symbol;
export type RuntimeTypePredicate<T> = (value: unknown) => value is T;
export declare class SerdeType<T> {
    #private;
    readonly name: string;
    private readonly predicate;
    constructor(name: string, predicate: RuntimeTypePredicate<T>);
    is(value: unknown): value is T;
    assert(value: unknown): asserts value is T;
    [registerSerde](registry: SerdeRegistry, serde: StreamSerde<T>): void;
    [resolveSerde](registry: SerdeRegistry): StreamSerde<T> | undefined;
}
export declare class SerdeRegistry {
    #private;
    register<T>(type: SerdeType<T>, serde: StreamSerde<T>): void;
    get<T>(type: SerdeType<T>): StreamSerde<T> | undefined;
    require<T>(type: SerdeType<T>): StreamSerde<T>;
    /** Resolve graph type metadata after TypeScript generic types have been erased. */
    requireByName<T>(name: string): StreamSerde<T>;
    matchesByName(name: string, value: unknown): boolean;
    assertByName<T>(name: string, value: unknown): asserts value is T;
    registerStreamErrorType<T>(streamId: number, type: SerdeType<T>): void;
    /** Registers generated graph type metadata lost to JavaScript type erasure. */
    registerStreamValueType<T>(streamId: number, type: SerdeType<T>): void;
    requireStreamValue<T>(streamId: number): StreamSerde<T>;
    requireStreamError<T>(streamId: number): StreamSerde<T>;
}
export {};
//# sourceMappingURL=registry.d.ts.map