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
  MissingOutcome: "missing_outcome",
  DuplicateTerminal: "duplicate_terminal",
  LateHeartbeat: "late_heartbeat",
  Suspended: "suspended"
} as const;

export type DurableCallEvent = (typeof DurableCallEvent)[keyof typeof DurableCallEvent];
export type DurableCallDiagnostics = (event: DurableCallEvent, error?: Error) => void;
export type DurableCallHeartbeatRecorder = (message: unknown) => void;

export class DurableCallContextError extends Error {}
export class NoDurableCallContextError extends DurableCallContextError {}
export class DurableCallAlreadyCompletedError extends DurableCallContextError {}
export class DurableCallHeartbeatAfterCompletionError extends DurableCallContextError {}
export class DurableCallOutcomeMissingError extends DurableCallContextError {}

export interface DurableContinuation {
  readonly version: 1;
  readonly fromName: string;
  readonly toName: string;
  readonly callId: string;
  readonly streamId: string;
  readonly priority: number;
  readonly deadlineUnixMillis: number;
  readonly wakeAtUnixMillis: number;
  readonly payload: Uint8Array;
}

export interface DurableActivityResult {
  readonly continuation?: DurableContinuation;
}

export class DurableCallContext {
  readonly #occurrences = new Map<string, number>();
  readonly #terminal = Promise.withResolvers<DurableActivityResult>();
  readonly #heartbeat: DurableCallHeartbeatRecorder | undefined;
  readonly #diagnostics: DurableCallDiagnostics | undefined;
  #completed = false;
  #outcome: Error | undefined;
  #span: Span | undefined;
  #spanEnded = false;
  #delayAtUnixMillis: number | undefined;
  #continuation: DurableContinuation | undefined;

  public constructor(
    public readonly parentCallId: string,
    heartbeat?: DurableCallHeartbeatRecorder,
    diagnostics?: DurableCallDiagnostics
  ) {
    this.#heartbeat = heartbeat;
    this.#diagnostics = diagnostics;
  }

  public occurrence(key: string): number {
    const next = (this.#occurrences.get(key) ?? 0) + 1;
    this.#occurrences.set(key, next);
    return next;
  }

  public bindSpan(span: Span): void {
    this.#span = span;
  }

  public heartbeat(message: unknown): void {
    if (this.#completed) {
      const error = new DurableCallHeartbeatAfterCompletionError(
        "durable call heartbeat after completion"
      );
      this.report(DurableCallEvent.LateHeartbeat, error);
      throw error;
    }
    this.#heartbeat?.(message);
    this.report(DurableCallEvent.Heartbeat);
  }

  public success(): void {
    this.complete(DurableCallEvent.Success);
  }

  public fail(error: Error): void {
    this.complete(DurableCallEvent.Error, error);
  }

  public cancelWithoutOutcome(cause: unknown): void {
    if (this.#completed) return;
    const detail = errorFromUnknown(cause);
    this.complete(
      DurableCallEvent.MissingOutcome,
      new DurableCallOutcomeMissingError(
        `durable call completed without explicit outcome${detail === undefined ? "" : `: ${detail.message}`}`,
        detail === undefined ? undefined : { cause: detail }
      )
    );
  }

  public async wait(): Promise<DurableActivityResult> {
    const result = await this.#terminal.promise;
    if (this.#outcome !== undefined) throw this.#outcome;
    return result;
  }

  public beginDelay(delayMs: number): void {
    if (this.#completed) {
      throw new DurableCallAlreadyCompletedError(
        "durable call is already completed; attempted delay"
      );
    }
    if (this.#delayAtUnixMillis !== undefined) {
      throw new DurableCallContextError("durable delay is already pending");
    }
    this.#delayAtUnixMillis = Date.now() + delayMs;
  }

  public captureContinuation(
    context: MessageContext,
    fromName: string,
    toName: string,
    payload: Uint8Array
  ): boolean {
    if (this.#delayAtUnixMillis === undefined) return false;
    if (this.#completed) {
      throw new DurableCallAlreadyCompletedError(
        "durable call is already completed; attempted suspension"
      );
    }
    const remainingMs = context.remainingMs();
    this.#continuation = {
      version: 1,
      fromName,
      toName,
      callId: `${this.parentCallId}/delay`,
      streamId: context.streamId() ?? "",
      priority: context.priority() ?? 0,
      deadlineUnixMillis:
        remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
      wakeAtUnixMillis: this.#delayAtUnixMillis,
      payload: Uint8Array.from(payload)
    };
    this.#completed = true;
    this.report(DurableCallEvent.Suspended);
    this.#terminal.resolve({ continuation: this.#continuation });
    return true;
  }

  public finishSpan(): void {
    if (this.#span === undefined || this.#spanEnded) return;
    this.#spanEnded = true;
    if (this.#outcome === undefined) this.#span.setStatus(SpanStatusCode.Ok, "");
    this.#span.end();
  }

  private complete(event: DurableCallEvent, outcome?: Error): void {
    if (this.#completed) {
      const error = new DurableCallAlreadyCompletedError(
        `durable call is already completed; attempted ${event}`
      );
      this.report(DurableCallEvent.DuplicateTerminal, error);
      throw error;
    }
    this.#completed = true;
    this.#outcome = outcome;
    this.report(event, outcome);
    this.#terminal.resolve({});
  }

  private report(event: DurableCallEvent, error?: Error): void {
    const attributes = [stringAttribute("event", event)];
    if (error !== undefined) attributes.push(stringAttribute("error", error.message));
    this.#span?.addEvent(`durable_call.${event}`, attributes);
    if (
      this.#span !== undefined &&
      (event === DurableCallEvent.Error || event === DurableCallEvent.MissingOutcome)
    ) {
      spanError(this.#span, error ?? new Error(event));
    }
    this.#diagnostics?.(event, error);
  }
}

function requireDurableCallContext(context: MessageContext, operation: string): DurableCallContext {
  const durable = context.durableCallContext();
  if (durable instanceof DurableCallContext) return durable;
  const error = new NoDurableCallContextError(
    `DurableCall ${operation} invoked outside an Activity`
  );
  process.emitWarning(error.message, { code: "SERVICELIB_DURABLE_CALL_CONTEXT" });
  throw error;
}

export function durableCallHeartbeat(context: MessageContext, message: unknown): void {
  requireDurableCallContext(context, DurableCallEvent.Heartbeat).heartbeat(message);
}

export function durableCallSuccess(context: MessageContext): void {
  requireDurableCallContext(context, DurableCallEvent.Success).success();
}

export function durableCallError(context: MessageContext, error: Error): void {
  requireDurableCallContext(context, DurableCallEvent.Error).fail(error);
}

export function beginDurableDelay(context: MessageContext, delayMs: number): boolean {
  const durable = context.durableCallContext();
  if (!(durable instanceof DurableCallContext)) return false;
  durable.beginDelay(delayMs);
  return true;
}

export function captureDurableContinuation(
  context: MessageContext,
  fromName: string,
  toName: string,
  payload: Uint8Array
): boolean {
  const durable = context.durableCallContext();
  if (!(durable instanceof DurableCallContext)) return false;
  return durable.captureContinuation(context, fromName, toName, payload);
}

export function bindDurableCallSpan(context: MessageContext, span: Span): boolean {
  const durable = context.durableCallContext();
  if (!(durable instanceof DurableCallContext)) return false;
  durable.bindSpan(span);
  return true;
}

export async function runDurableCallActivity(
  signal: AbortSignal,
  durable: DurableCallContext,
  invoke: () => Promise<void>
): Promise<DurableActivityResult> {
  const cancelled = (): void => {
    durable.cancelWithoutOutcome(signal.reason);
  };
  signal.addEventListener("abort", cancelled, { once: true });
  if (signal.aborted) cancelled();
  try {
    try {
      await invoke();
    } catch (error: unknown) {
      if (signal.aborted) durable.cancelWithoutOutcome(error);
      else {
        try {
          durable.fail(errorFromUnknown(error) ?? new Error("unknown DurableCall failure"));
        } catch (terminalError: unknown) {
          if (!(terminalError instanceof DurableCallAlreadyCompletedError)) throw terminalError;
        }
      }
    }
    return await durable.wait();
  } finally {
    signal.removeEventListener("abort", cancelled);
    durable.finishSpan();
  }
}

function errorFromUnknown(value: unknown): Error | undefined {
  if (value === undefined) return undefined;
  if (value instanceof Error) return value;
  if (typeof value === "string") return new Error(value);
  return new Error("non-Error DurableCall failure");
}
