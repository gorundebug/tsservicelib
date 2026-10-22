/** Resolve the When node identity once in buildSwitch, not for every message. */
export function whenIndex(whenItems, name) {
    if (name.length === 0)
        throw new Error("case branch name must not be empty");
    let index = -1;
    for (const [candidate, item] of whenItems.entries()) {
        if (item.name !== name)
            continue;
        if (index >= 0) {
            throw new Error(`case branch ${JSON.stringify(name)} is ambiguous (indices ${String(index)} and ${String(candidate)})`);
        }
        index = candidate;
    }
    if (index < 0)
        throw new Error(`case branch ${JSON.stringify(name)} was not found`);
    return index;
}
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