import {} from "../environment/log.js";
class TestLogger {
    #record;
    constructor(record) {
        this.#record = record;
    }
    debug(context, message, ...fields) {
        void context;
        this.#record({ level: "debug", message, fields: [...fields] });
    }
    info(context, message, ...fields) {
        void context;
        this.#record({ level: "info", message, fields: [...fields] });
    }
    warn(context, message, ...fields) {
        void context;
        this.#record({ level: "warn", message, fields: [...fields] });
    }
    error(context, message, ...fields) {
        void context;
        this.#record({ level: "error", message, fields: [...fields] });
    }
}
export class TestLog {
    #entries = [];
    #logger = new TestLogger((entry) => this.#entries.push(entry));
    defaultLogger(config) {
        void config;
        return this.#logger;
    }
    shutdown(context) {
        void context;
        return Promise.resolve();
    }
    entries() {
        return this.#entries.map((entry) => ({ ...entry, fields: [...entry.fields] }));
    }
    entriesAtLevel(level) {
        return this.entries().filter((entry) => entry.level === level);
    }
    reset() {
        this.#entries.length = 0;
    }
}
//# sourceMappingURL=index.js.map