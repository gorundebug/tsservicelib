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

export class DurableCallContextError extends Error {}
export class DurableCallHeartbeatAfterCompletionError extends DurableCallContextError {}

/** Processing-side state for one Temporal endpoint Activity. */
export class DurableCallContext {
  readonly #heartbeat: DurableCallHeartbeatRecorder | undefined;
  readonly #diagnostics: DurableCallDiagnostics | undefined;
  #closed = false;
  #span: Span | undefined;
  #spanEnded = false;

  public constructor(
    public readonly messageId: string,
    heartbeat?: DurableCallHeartbeatRecorder,
    diagnostics?: DurableCallDiagnostics
  ) {
    this.#heartbeat = heartbeat;
    this.#diagnostics = diagnostics;
  }

  public bindSpan(span: Span): void {
    this.#span = span;
  }

  public heartbeat(message: unknown): void {
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
    const attributes = [stringAttribute("event", event)];
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

function errorFromUnknown(value: unknown): Error {
  if (value instanceof Error) return value;
  if (typeof value === "string") return new Error(value);
  return new Error("non-Error Temporal Activity failure");
}
