/** Indexed min-heap: removal and reprioritization never scan the queue. */
export class IndexedHeap<T extends object> {
  readonly #items: T[] = [];
  readonly #positions = new Map<T, number>();
  public constructor(private readonly compare: (left: T, right: T) => number) {}
  public get length(): number {
    return this.#items.length;
  }
  public peek(): T | undefined {
    return this.#items[0];
  }
  public has(item: T): boolean {
    return this.#positions.has(item);
  }
  public push(item: T): void {
    this.#positions.set(item, this.#items.length);
    this.#items.push(item);
    this.up(this.#items.length - 1);
  }
  public pop(): T | undefined {
    const item = this.peek();
    if (item !== undefined) this.remove(item);
    return item;
  }
  public remove(item: T): boolean {
    const index = this.#positions.get(item);
    if (index === undefined) return false;
    const last = this.#items.pop();
    this.#positions.delete(item);
    if (last !== undefined && index < this.#items.length) {
      this.#items[index] = last;
      this.#positions.set(last, index);
      this.fix(last);
    }
    return true;
  }
  public fix(item: T): void {
    const index = this.#positions.get(item);
    if (index !== undefined) this.down(this.up(index));
  }
  private swap(a: number, b: number): void {
    const left = this.#items[a];
    const right = this.#items[b];
    if (left === undefined || right === undefined) throw new Error("invalid heap index");
    this.#items[a] = right;
    this.#items[b] = left;
    this.#positions.set(right, a);
    this.#positions.set(left, b);
  }
  private up(index: number): number {
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      const item = this.#items[index];
      const other = this.#items[parent];
      if (item === undefined || other === undefined || this.compare(item, other) >= 0) break;
      this.swap(index, parent);
      index = parent;
    }
    return index;
  }
  private down(index: number): void {
    for (;;) {
      let child = index * 2 + 1;
      const left = this.#items[child];
      if (left === undefined) return;
      const right = this.#items[child + 1];
      if (right !== undefined && this.compare(right, left) < 0) child += 1;
      const item = this.#items[index];
      const smallest = this.#items[child];
      if (item === undefined || smallest === undefined || this.compare(item, smallest) <= 0) return;
      this.swap(index, child);
      index = child;
    }
  }
}
