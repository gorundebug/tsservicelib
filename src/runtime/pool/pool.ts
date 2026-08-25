import type { Context } from "../context.js";
import type { Completion } from "../stream.js";
import type { Logger, Metrics } from "../environment/index.js";

export class PoolStoppedError extends Error {
  public constructor(name: string) {
    super(`pool ${name} is stopped`);
    this.name = "PoolStoppedError";
  }
}

export type PoolTask = () => Completion;

/** Runtime contract used by callers; implementations may be process or Workflow local. */
export interface TaskPoolLike {
  name(): string;
  executorsCount(): number;
  queueLength(): number;
  activeCount(): number;
  addTask(context: Context, execute: PoolTask): void;
}

/** Priority extension of the portable caller-facing pool contract. */
export interface PriorityTaskPoolLike {
  name(): string;
  executorsCount(): number;
  queueLength(): number;
  activeCount(): number;
  addTask(context: Context, priority: number, execute: PoolTask): void;
}

export interface TaskPoolOptions {
  readonly name: string;
  readonly executorsCount: number;
  readonly onError?: (error: unknown) => void;
  readonly logger?: Logger;
  readonly metrics?: Metrics;
  readonly service?: string;
}
