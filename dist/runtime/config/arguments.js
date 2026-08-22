const DEFAULT_CONFIG_PATH = "./config/config.yaml";
const DEFAULT_VALUES_PATH = "./config/overrides.yaml";
function readValue(arguments_, index, option) {
    const value = arguments_[index + 1];
    if (value === undefined || value.startsWith("-")) {
        throw new Error(`missing value for ${option}`);
    }
    return value;
}
/** Parses the same service entry-point flags used by the canonical examples. */
export function parseConfigArguments(arguments_) {
    let configPath = DEFAULT_CONFIG_PATH;
    let valuesPath = DEFAULT_VALUES_PATH;
    for (let index = 0; index < arguments_.length; index += 1) {
        const argument = arguments_[index];
        if (argument === "--config" || argument === "-config") {
            configPath = readValue(arguments_, index, argument);
            index += 1;
        }
        else if (argument?.startsWith("--config=")) {
            configPath = argument.slice("--config=".length);
        }
        else if (argument === "--values" || argument === "-values") {
            valuesPath = readValue(arguments_, index, argument);
            index += 1;
        }
        else if (argument?.startsWith("--values=")) {
            valuesPath = argument.slice("--values=".length);
        }
        else {
            throw new Error(`unknown command-line argument: ${String(argument)}`);
        }
    }
    if (configPath.length === 0) {
        throw new Error("config path must not be empty");
    }
    if (valuesPath.length === 0) {
        throw new Error("values path must not be empty");
    }
    return { configPath, valuesPath };
}
//# sourceMappingURL=arguments.js.map