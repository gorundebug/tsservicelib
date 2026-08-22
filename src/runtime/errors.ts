export class RuntimeStoppedError extends Error {
  public constructor(message = "runtime is not accepting new work") {
    super(message);
    this.name = "RuntimeStoppedError";
  }
}

export class RuntimeDrainTimeoutError extends Error {
  public constructor(public readonly timeoutMs: number) {
    super(`runtime did not drain accepted work within ${String(timeoutMs)}ms`);
    this.name = "RuntimeDrainTimeoutError";
  }
}

export function errorFromUnknown(value: unknown): Error {
  if (value instanceof Error) {
    return value;
  }
  return new Error(String(value));
}
