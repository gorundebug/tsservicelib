const transformationNames = {
    Case: "case",
    CycleLink: "cycleLink",
    Delay: "delay",
    Error: "error",
    Filter: "filter",
    FlatMap: "flatMap",
    FlatMapIterable: "flatMapIterable",
    Input: "input",
    Join: "join",
    KeyBy: "keyBy",
    Map: "map",
    Merge: "merge",
    MultiJoin: "multiJoin",
    Process: "process",
    Sink: "sink",
    Split: "split",
    When: "when"
};
export function transformationName(type) {
    return transformationNames[type];
}
export const JoinType = {
    Undefined: 0,
    Inner: 1,
    Left: 2,
    Right: 3,
    Outer: 4
};
export const JoinStorageType = {
    Undefined: 0,
    HashMap: 1,
    RocksDB: 2,
    Aerospike: 3
};
export const DataConnectorType = {
    Undefined: 0,
    HTTP: 1,
    GRPC: 2,
    Kafka: 3,
    Custom: 4,
    Cron: 5,
    Temporal: 6
};
export const HTTPMethodType = {
    Undefined: "",
    GET: "GET",
    POST: "POST"
};
//# sourceMappingURL=types.js.map