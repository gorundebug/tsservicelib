/** Indexed min-heap: removal and reprioritization never scan the queue. */
export class IndexedHeap {
    compare;
    #items = [];
    #positions = new Map();
    constructor(compare) {
        this.compare = compare;
    }
    get length() {
        return this.#items.length;
    }
    peek() {
        return this.#items[0];
    }
    has(item) {
        return this.#positions.has(item);
    }
    push(item) {
        this.#positions.set(item, this.#items.length);
        this.#items.push(item);
        this.up(this.#items.length - 1);
    }
    pop() {
        const item = this.peek();
        if (item !== undefined)
            this.remove(item);
        return item;
    }
    remove(item) {
        const index = this.#positions.get(item);
        if (index === undefined)
            return false;
        const last = this.#items.pop();
        this.#positions.delete(item);
        if (last !== undefined && index < this.#items.length) {
            this.#items[index] = last;
            this.#positions.set(last, index);
            this.fix(last);
        }
        return true;
    }
    fix(item) {
        const index = this.#positions.get(item);
        if (index !== undefined)
            this.down(this.up(index));
    }
    swap(a, b) {
        const left = this.#items[a];
        const right = this.#items[b];
        if (left === undefined || right === undefined)
            throw new Error("invalid heap index");
        this.#items[a] = right;
        this.#items[b] = left;
        this.#positions.set(right, a);
        this.#positions.set(left, b);
    }
    up(index) {
        while (index > 0) {
            const parent = Math.floor((index - 1) / 2);
            const item = this.#items[index];
            const other = this.#items[parent];
            if (item === undefined || other === undefined || this.compare(item, other) >= 0)
                break;
            this.swap(index, parent);
            index = parent;
        }
        return index;
    }
    down(index) {
        for (;;) {
            let child = index * 2 + 1;
            const left = this.#items[child];
            if (left === undefined)
                return;
            const right = this.#items[child + 1];
            if (right !== undefined && this.compare(right, left) < 0)
                child += 1;
            const item = this.#items[index];
            const smallest = this.#items[child];
            if (item === undefined || smallest === undefined || this.compare(item, smallest) <= 0)
                return;
            this.swap(index, child);
            index = child;
        }
    }
}
//# sourceMappingURL=indexed-heap.js.map