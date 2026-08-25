import type { MessageContext } from "./context.js";
import {
  SpanStatusCode,
  spanError,
  stringAttribute,
  type Span
} from "./environment/tracing/index.js";

export const DurableCallEvent = {
  Heartbeat: "heartbeat",
  Success: "success",
  Error: "error",
  LateHeartbeat: "late_heartbeat"
} as const;

export type DurableCallEvent = (typeof DurableCallEvent)[keyof typeof DurableCallEvent];
export type DurableCallDiagnostics = (event: DurableCallEvent, error?: Error) => void;
export type DurableCallHeartbeatRecorder = (message: unknown) => void;
export type DurableCallTimer = (delayMs: number) => Promise<void>;
export type DurableCallExecutionType = "Activity" | "Workflow";

export interface DurableCallContextOptions {
  readonly executionType: DurableCallExecutionType;
  readonly heartbeat?: DurableCallHeartbeatRecorder | undefined;
  readonly timer?: DurableCallTimer | undefined;
  readonly diagnostics?: DurableCallDiagnostics | undefined;
}

export class DurableCallContextError extends Error {}
export class DurableCallHeartbeatAfterCompletionError extends DurableCallContextError {}
export class TemporalContinueAsNewRequest extends Error {
  public constructor(public readonly nextInput: unknown) {
    super("Temporal Continue-As-New");
  }
}

/** Processing-side state for one Temporal endpoint Activity. */
export class DurableCallContext {
  readonly #heartbeat: DurableCallHeartbeatRecorder | undefined;
  readonly #timer: DurableCallTimer | undefined;
  readonly #diagnostics: DurableCallDiagnostics | undefined;
  #closed = false;
  #span: Span | undefined;
  #spanEnded = false;

  public constructor(
    public readonly messageId: string,
    public readonly executionType: DurableCallExecutionType,
    options: Omit<DurableCallContextOptions, "executionType"> = {}
  ) {
    this.#heartbeat = options.heartbeat;
    this.#timer = options.timer;
    this.#diagnostics = options.diagnostics;
    if (executionType === "Activity" && this.#timer !== undefined) {
      throw new Error("Temporal Activity durable context cannot own a Workflow timer");
    }
    if (executionType === "Workflow" && this.#heartbeat !== undefined) {
      throw new Error("Temporal Workflow durable context cannot own an Activity heartbeat");
    }
  }

  public bindSpan(span: Span): void {
    this.#span = span;
  }

  public heartbeat(message: unknown): void {
    if (this.executionType !== "Activity") return;
    if (this.#closed) {
      const error = new DurableCallHeartbeatAfterCompletionError(
        "durable call heartbeat after completion"
      );
      this.report(DurableCallEvent.LateHeartbeat, error);
      throw error;
    }
    this.#heartbeat?.(message);
    this.report(DurableCallEvent.Heartbeat);
  }

  public async delay(delayMs: number): Promise<boolean> {
    if (this.executionType !== "Workflow") return false;
    if (this.#closed) throw new DurableCallContextError("durable call timer after completion");
    if (this.#timer === undefined) {
      throw new DurableCallContextError("Temporal Workflow durable timer is not configured");
    }
    await this.#timer(delayMs);
    return true;
  }

  public continueAsNew(nextInput: unknown): never {
    if (this.executionType !== "Workflow") {
      throw new DurableCallContextError("Temporal Continue-As-New requires a Workflow endpoint");
    }
    if (this.#closed) {
      throw new DurableCallContextError("Temporal Continue-As-New after Workflow completion");
    }
    throw new TemporalContinueAsNewRequest(nextInput);
  }

  public close(error?: Error): void {
    if (this.#closed) return;
    this.#closed = true;
    this.report(error === undefined ? DurableCallEvent.Success : DurableCallEvent.Error, error);
    if (this.#span !== undefined && !this.#spanEnded) {
      this.#spanEnded = true;
      if (error === undefined) this.#span.setStatus(SpanStatusCode.Ok, "");
      this.#span.end();
    }
  }

  private report(event: DurableCallEvent, error?: Error): void {
    const attributes: ReturnType<typeof stringAttribute>[] = [];
    if (error !== undefined) attributes.push(stringAttribute("error", error.message));
    this.#span?.addEvent(`temporal.activity.${event}`, attributes);
    if (this.#span !== undefined && event === DurableCallEvent.Error) {
      spanError(this.#span, error ?? new Error(event));
    }
    this.#diagnostics?.(event, error);
  }
}

export function durableCallHeartbeat(context: MessageContext, message: unknown): void {
  const durable = context.durableCallContext();
  if (durable instanceof DurableCallContext) durable.heartbeat(message);
}

/** Terminate the current Workflow run with a new typed endpoint input. */
export function temporalContinueAsNew(context: MessageContext, nextInput: unknown): never {
  const durable = context.durableCallContext();
  if (!(durable instanceof DurableCallContext)) {
    throw new DurableCallContextError("Temporal Continue-As-New requires a Workflow endpoint");
  }
  return durable.continueAsNew(nextInput);
}

/** Returns true when a Temporal Workflow timer handled the delay. */
export async function durableCallDelay(context: MessageContext, delayMs: number): Promise<boolean> {
  const durable = context.durableCallContext();
  return durable instanceof DurableCallContext ? durable.delay(delayMs) : false;
}

export function bindDurableCallSpan(context: MessageContext, span: Span): boolean {
  const durable = context.durableCallContext();
  if (!(durable instanceof DurableCallContext)) return false;
  durable.bindSpan(span);
  return true;
}

export async function runDurableCallActivity<T>(
  durable: DurableCallContext,
  invoke: () => Promise<T>
): Promise<T> {
  try {
    const result = await invoke();
    durable.close();
    return result;
  } catch (error: unknown) {
    const failure = errorFromUnknown(error);
    durable.close(failure);
    throw failure;
  }
}

export async function runDurableCallWorkflow<T>(
  durable: DurableCallContext,
  invoke: () => Promise<T>
): Promise<T> {
  if (durable.executionType !== "Workflow") {
    throw new DurableCallContextError("runDurableCallWorkflow requires a Workflow context");
  }
  try {
    const result = await invoke();
    durable.close();
    return result;
  } catch (error: unknown) {
    if (error instanceof TemporalContinueAsNewRequest) {
      durable.close();
      throw error;
    }
    const failure = errorFromUnknown(error);
    durable.close(failure);
    throw failure;
  }
}

function errorFromUnknown(value: unknown): Error {
  if (value instanceof Error) return value;
  if (typeof value === "string") return new Error(value);
  return new Error("non-Error Temporal Activity failure");
}
