export class TaskPoolCaller {
    #pool;
    #consumer;
    #onRejected;
    constructor(pool, consumer, onRejected = () => undefined) {
        this.#pool = pool;
        this.#consumer = consumer;
        this.#onRejected = onRejected;
    }
    isAsync() {
        return true;
    }
    consume(context, value) {
        try {
            this.#pool.addTask(context, () => this.#consumer.consume(context, value));
        }
        catch (error) {
            this.#onRejected(error);
        }
    }
}
export class PriorityTaskPoolCaller {
    #pool;
    #consumer;
    #defaultPriority;
    #onRejected;
    constructor(pool, consumer, defaultPriority, onRejected = () => undefined) {
        this.#pool = pool;
        this.#consumer = consumer;
        this.#defaultPriority = defaultPriority;
        this.#onRejected = onRejected;
    }
    isAsync() {
        return true;
    }
    consume(context, value) {
        try {
            this.#pool.addTask(context, context.priority() ?? this.#defaultPriority, () => this.#consumer.consume(context, value));
        }
        catch (error) {
            this.#onRejected(error);
        }
    }
}
export class ParallelCaller {
    #tasks;
    #consumer;
    constructor(tasks, consumer) {
        this.#tasks = tasks;
        this.#consumer = consumer;
    }
    isAsync() {
        return true;
    }
    consume(context, value) {
        this.#tasks.admitDetached(async (signal) => {
            await this.#consumer.consume(context.withExternalCancellation(signal), value);
        });
    }
}
//# sourceMappingURL=caller.js.map