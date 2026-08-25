import { JoinStorageType } from "../config/types.js";
import { HashMapJoinStorage } from "./hash-map-join-storage.js";
export function makeJoinStorage(storageType, environment, config) {
    switch (storageType) {
        case JoinStorageType.HashMap:
            return new HashMapJoinStorage(environment, config);
        default:
            throw new Error(`join storage type ${String(storageType)} is not supported`);
    }
}
//# sourceMappingURL=join-storage.js.map