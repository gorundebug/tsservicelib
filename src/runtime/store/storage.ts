import type { Context } from "../context.js";

export interface Storage {
  start(context: Context): void | Promise<void>;
  stop(context: Context): void | Promise<void>;
}

export class StoreAlreadyStartedError extends Error {
  public constructor() {
    super("store already started");
    this.name = "StoreAlreadyStartedError";
  }
}

export class StoreNotStartedError extends Error {
  public constructor() {
    super("store not started");
    this.name = "StoreNotStartedError";
  }
}

export class StoreStoppedError extends Error {
  public constructor() {
    super("store stopped");
    this.name = "StoreStoppedError";
  }
}

export class DuplicateKeyError extends Error {
  public constructor(key: unknown) {
    super(`duplicate key ${String(key)}`);
    this.name = "DuplicateKeyError";
  }
}
