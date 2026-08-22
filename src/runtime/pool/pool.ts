import type { Completion } from "../stream.js";
import type { Logger, Metrics } from "../environment/index.js";

export class PoolStoppedError extends Error {
  public constructor(name: string) {
    super(`pool ${name} is stopped`);
    this.name = "PoolStoppedError";
  }
}

export type PoolTask = () => Completion;

export interface TaskPoolOptions {
  readonly name: string;
  readonly executorsCount: number;
  readonly onError?: (error: unknown) => void;
  readonly logger?: Logger;
  readonly metrics?: Metrics;
  readonly service?: string;
}
