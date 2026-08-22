import { type SerdeLimits } from "./serde.js";
export declare const sizeBytes = 8;
export declare function appendBytes(prefix: Uint8Array | undefined, value: Uint8Array): Uint8Array;
export declare function encodeSize(size: number): Uint8Array;
export declare class SerdeReader {
    #private;
    constructor(data: Uint8Array, maxTotalBytes: number);
    get offset(): number;
    read(size: number, what: string): Uint8Array;
    readSize(maximum: number, what: string): number;
}
export declare function validateLimits(limits: SerdeLimits): void;
export declare function view(data: Uint8Array): DataView;
//# sourceMappingURL=framing.d.ts.map