import { type CanonicalConfig } from "../config/index.js";
import type { RuntimeEnvironment } from "../environment/index.js";
export interface StatusNode {
    readonly id: number;
    readonly label: string;
    readonly shape: "image";
    readonly image: {
        readonly unselected: string;
        readonly selected: string;
    };
    readonly size: 30;
    readonly color: {
        readonly border: "transparent";
        readonly highlight: {
            readonly border: "transparent";
        };
    };
    readonly opacity: 1;
    readonly x: number;
    readonly y: number;
}
export interface StatusEdge {
    readonly from: number;
    readonly to: number;
    readonly arrows: "to";
    readonly length: 200;
    readonly label: string;
    readonly color: {
        readonly opacity: 1;
        readonly color: string;
    };
}
export interface StatusNetworkData {
    readonly nodes: readonly StatusNode[];
    readonly edges: readonly StatusEdge[];
}
export declare function makeStatusNetworkData(environment: RuntimeEnvironment): StatusNetworkData;
/** Reconstruct the canonical graph from runtime-owned components, as Go does. */
export declare function runtimeToCanonicalConfig(environment: RuntimeEnvironment): CanonicalConfig;
//# sourceMappingURL=graph.d.ts.map