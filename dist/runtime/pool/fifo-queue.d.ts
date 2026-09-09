/** FIFO with constant-time extraction and cancellation promotion. */
export declare class FifoQueue<T extends object> {
    #private;
    get length(): number;
    push(value: T): void;
    shift(): T | undefined;
    moveToFront(value: T): boolean;
}
//# sourceMappingURL=fifo-queue.d.ts.map