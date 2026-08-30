export interface ConfigPaths {
    readonly configPath: string;
    readonly valuesPath: string;
    readonly overridesPath?: string;
}
/** Parses the same service entry-point flags used by the canonical examples. */
export declare function parseConfigArguments(arguments_: readonly string[]): ConfigPaths;
//# sourceMappingURL=arguments.d.ts.map