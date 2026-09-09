/** Indexed min-heap: removal and reprioritization never scan the queue. */
export declare class IndexedHeap<T extends object> {
    #private;
    private readonly compare;
    constructor(compare: (left: T, right: T) => number);
    get length(): number;
    peek(): T | undefined;
    has(item: T): boolean;
    push(item: T): void;
    pop(): T | undefined;
    remove(item: T): boolean;
    fix(item: T): void;
    private swap;
    private up;
    private down;
}
//# sourceMappingURL=indexed-heap.d.ts.map