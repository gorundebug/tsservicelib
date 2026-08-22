/** JavaScript equivalent of Go's runtime-type Case selector. */
export function defaultBuildSwitch(stream, whenItems) {
    const registry = stream.runtimeEnvironment().serdeRegistry();
    return (value) => {
        let selected;
        for (const [index, when] of whenItems.entries()) {
            if (registry.matchesByName(when.valueType(), value))
                selected = index;
        }
        if (selected === undefined) {
            throw new TypeError(`unknown value type in case switch for stream ${stream.name}`);
        }
        return selected;
    };
}
//# sourceMappingURL=functions.js.map