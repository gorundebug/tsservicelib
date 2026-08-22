import { SerdeError, type SerdeLimits } from "./serde.js";

export const sizeBytes = 8;

export function appendBytes(prefix: Uint8Array | undefined, value: Uint8Array): Uint8Array {
  if (prefix === undefined || prefix.byteLength === 0) {
    return value;
  }
  const result = new Uint8Array(prefix.byteLength + value.byteLength);
  result.set(prefix);
  result.set(value, prefix.byteLength);
  return result;
}

export function encodeSize(size: number): Uint8Array {
  if (!Number.isSafeInteger(size) || size < 0) {
    throw new SerdeError("size must be a non-negative safe integer", 0);
  }
  const result = new Uint8Array(sizeBytes);
  new DataView(result.buffer).setBigUint64(0, BigInt(size), false);
  return result;
}

export class SerdeReader {
  readonly #data: Uint8Array;
  #offset = 0;

  public constructor(data: Uint8Array, maxTotalBytes: number) {
    validateLimit(maxTotalBytes, "maxTotalBytes");
    if (data.byteLength > maxTotalBytes) {
      throw new SerdeError("serde input exceeds configured limit", 0);
    }
    this.#data = data;
  }

  public get offset(): number {
    return this.#offset;
  }

  public read(size: number, what: string): Uint8Array {
    if (!Number.isSafeInteger(size) || size < 0) {
      throw new SerdeError(`${what} has invalid size`, this.#offset);
    }
    const end = this.#offset + size;
    if (!Number.isSafeInteger(end)) {
      throw new SerdeError("serde size overflow", this.#offset);
    }
    if (end > this.#data.byteLength) {
      throw new SerdeError(`serde underflow while reading ${what}`, this.#offset);
    }
    const result = this.#data.subarray(this.#offset, end);
    this.#offset = end;
    return result;
  }

  public readSize(maximum: number, what: string): number {
    validateLimit(maximum, what);
    const offset = this.#offset;
    const encoded = view(this.read(sizeBytes, what)).getBigUint64(0, false);
    if (encoded > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new SerdeError(`${what} does not fit a safe integer`, offset);
    }
    const size = Number(encoded);
    if (size > maximum) {
      throw new SerdeError(`${what} exceeds configured limit`, offset);
    }
    return size;
  }
}

export function validateLimits(limits: SerdeLimits): void {
  validateLimit(limits.maxStringBytes, "maxStringBytes");
  validateLimit(limits.maxBytes, "maxBytes");
  validateLimit(limits.maxContainerElements, "maxContainerElements");
  validateLimit(limits.maxTotalBytes, "maxTotalBytes");
}

export function view(data: Uint8Array): DataView {
  return new DataView(data.buffer, data.byteOffset, data.byteLength);
}

function validateLimit(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
}
