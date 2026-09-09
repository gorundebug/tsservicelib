/** FIFO with constant-time extraction and cancellation promotion. */
export class FifoQueue {
    #nodes = new Map();
    #head;
    #tail;
    get length() {
        return this.#nodes.size;
    }
    push(value) {
        const node = { value, previous: this.#tail, next: undefined };
        if (this.#tail !== undefined)
            this.#tail.next = node;
        else
            this.#head = node;
        this.#tail = node;
        this.#nodes.set(value, node);
    }
    shift() {
        const node = this.#head;
        if (node === undefined)
            return undefined;
        this.#head = node.next;
        if (this.#head !== undefined)
            this.#head.previous = undefined;
        else
            this.#tail = undefined;
        this.#nodes.delete(node.value);
        return node.value;
    }
    moveToFront(value) {
        const node = this.#nodes.get(value);
        if (node === undefined || node === this.#head)
            return false;
        if (node.previous !== undefined)
            node.previous.next = node.next;
        if (node.next !== undefined)
            node.next.previous = node.previous;
        else
            this.#tail = node.previous;
        node.previous = undefined;
        node.next = this.#head;
        if (this.#head !== undefined)
            this.#head.previous = node;
        this.#head = node;
        return true;
    }
}
//# sourceMappingURL=fifo-queue.js.map