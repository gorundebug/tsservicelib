import type { Context as OpenTelemetryContext } from "@opentelemetry/api";

export const STREAM_ID_HEADER = "x-stream-id";
export const TRACE_SAMPLING_HEADER = "x-trace";

export function newStreamId(): string {
  return globalThis.crypto.randomUUID();
}

interface ContextState {
  readonly signal: AbortSignal;
  readonly deadline: number | undefined;
  readonly samplingEnabled: boolean;
}

interface MessageContextState extends ContextState {
  readonly durableCallContext: DurableCallExecutionContext | undefined;
  readonly metadata: ReadonlyMap<string, string> | undefined;
  readonly openTelemetryContext: OpenTelemetryContext | undefined;
  readonly priority: number | undefined;
}

/** Minimal structural contract kept by MessageContext without a module cycle. */
export interface DurableCallExecutionContext {
  readonly messageId: string;
}

const EMPTY_METADATA: ReadonlyMap<string, string> = new Map();

/** Monotonic on Node.js and deterministic inside a Temporal Workflow isolate. */
function contextNow(): number {
  const performanceValue: unknown = Reflect.get(globalThis, "performance");
  if (typeof performanceValue === "object" && performanceValue !== null) {
    const now: unknown = Reflect.get(performanceValue, "now");
    if (typeof now === "function") return Number(Reflect.apply(now, performanceValue, []));
  }
  return Date.now();
}

function deadlineSignal(deadline: number | undefined): AbortSignal | undefined {
  if (deadline === undefined) {
    return undefined;
  }
  const controller = new AbortController();
  const timer = setTimeout(
    () => {
      controller.abort(new Error("context deadline exceeded"));
    },
    Math.max(0, Math.ceil(deadline - contextNow()))
  );
  (timer as unknown as { unref?: () => void }).unref?.();
  return controller.signal;
}

function composeSignal(signal: AbortSignal, deadline: number | undefined): AbortSignal {
  const timeoutSignal = deadlineSignal(deadline);
  return timeoutSignal === undefined ? signal : combineAbortSignals([signal, timeoutSignal]);
}

/** Portable equivalent of AbortSignal.any for runtimes such as Temporal isolates. */
export function combineAbortSignals(signals: readonly AbortSignal[]): AbortSignal {
  if (signals.length === 0) return new AbortController().signal;
  const first = signals[0];
  if (signals.length === 1 && first !== undefined) return first;
  const controller = new AbortController();
  const abort = (signal: AbortSignal): void => {
    if (!controller.signal.aborted) controller.abort(signal.reason);
    for (const candidate of signals) {
      const listener = listeners.get(candidate);
      if (listener !== undefined) candidate.removeEventListener("abort", listener);
    }
  };
  const listeners = new Map<AbortSignal, () => void>();
  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort(signal.reason);
      return controller.signal;
    }
    const listener = (): void => {
      abort(signal);
    };
    listeners.set(signal, listener);
    signal.addEventListener("abort", listener, { once: true });
  }
  return controller.signal;
}

function boundedDeadline(current: number | undefined, timeoutMs: number): number {
  const candidate = contextNow() + Math.max(0, timeoutMs);
  return current === undefined ? candidate : Math.min(current, candidate);
}

function childDeadline(
  current: number | undefined,
  requested: number | undefined
): number | undefined {
  if (requested === undefined) return current;
  return current === undefined ? requested : Math.min(current, requested);
}

export class Context {
  #state: ContextState;

  public constructor(signal: AbortSignal = new AbortController().signal) {
    this.#state = { signal, deadline: undefined, samplingEnabled: false };
  }

  public static background(): Context {
    return new Context();
  }

  private static fromState(state: ContextState): Context {
    const context = new Context(state.signal);
    context.#state = state;
    return context;
  }

  protected state(): ContextState {
    return this.#state;
  }

  public signal(): AbortSignal {
    return this.#state.signal;
  }

  public deadline(): number | undefined {
    return this.#state.deadline;
  }

  public remainingMs(): number | undefined {
    return this.#state.deadline === undefined
      ? undefined
      : Math.max(0, this.#state.deadline - contextNow());
  }

  public cancelled(): boolean {
    return this.#state.signal.aborted || (this.#state.deadline ?? Infinity) <= contextNow();
  }

  public samplingEnabled(): boolean {
    return this.#state.samplingEnabled;
  }

  public withDeadline(deadline: number | undefined): Context {
    const effective = childDeadline(this.#state.deadline, deadline);
    if (effective === this.#state.deadline) return this;
    return Context.fromState({
      ...this.#state,
      deadline: effective,
      signal: composeSignal(this.#state.signal, effective)
    });
  }

  public bounded(timeoutMs: number): Context {
    return this.withDeadline(boundedDeadline(this.#state.deadline, timeoutMs));
  }

  public withExternalCancellation(signal: AbortSignal): Context {
    if (signal === this.#state.signal) return this;
    return Context.fromState({
      ...this.#state,
      signal: combineAbortSignals([this.#state.signal, signal])
    });
  }

  public withoutCancellation(): Context {
    return Context.fromState({
      ...this.#state,
      signal: new AbortController().signal,
      deadline: undefined
    });
  }

  public withSampling(enabled: boolean): Context {
    return Context.fromState({ ...this.#state, samplingEnabled: enabled });
  }
}

export class MessageContext extends Context {
  #messageState: MessageContextState;

  public constructor(signal: AbortSignal = new AbortController().signal) {
    super(signal);
    this.#messageState = {
      ...this.state(),
      durableCallContext: undefined,
      metadata: undefined,
      openTelemetryContext: undefined,
      priority: undefined
    };
  }

  private static fromMessageState(state: MessageContextState): MessageContext {
    const context = new MessageContext(state.signal);
    context.#messageState = state;
    return context;
  }

  private clone(changes: Partial<MessageContextState>): MessageContext {
    return MessageContext.fromMessageState({ ...this.#messageState, ...changes });
  }

  public override signal(): AbortSignal {
    return this.#messageState.signal;
  }

  public override deadline(): number | undefined {
    return this.#messageState.deadline;
  }

  public override remainingMs(): number | undefined {
    return this.#messageState.deadline === undefined
      ? undefined
      : Math.max(0, this.#messageState.deadline - contextNow());
  }

  public override cancelled(): boolean {
    return (
      this.#messageState.signal.aborted || (this.#messageState.deadline ?? Infinity) <= contextNow()
    );
  }

  public override samplingEnabled(): boolean {
    return this.#messageState.samplingEnabled;
  }

  public override withDeadline(deadline: number | undefined): MessageContext {
    const effective = childDeadline(this.#messageState.deadline, deadline);
    if (effective === this.#messageState.deadline) return this;
    return this.clone({
      deadline: effective,
      signal: composeSignal(this.#messageState.signal, effective)
    });
  }

  public override bounded(timeoutMs: number): MessageContext {
    return this.withDeadline(boundedDeadline(this.#messageState.deadline, timeoutMs));
  }

  public override withExternalCancellation(signal: AbortSignal): MessageContext {
    if (signal === this.#messageState.signal) return this;
    return this.clone({ signal: combineAbortSignals([this.#messageState.signal, signal]) });
  }

  public override withoutCancellation(): MessageContext {
    return this.clone({
      signal: new AbortController().signal,
      deadline: undefined
    });
  }

  public override withSampling(enabled: boolean): MessageContext {
    const metadata = new Map(this.metadata());
    if (enabled) {
      metadata.set(TRACE_SAMPLING_HEADER, "1");
    } else {
      metadata.delete(TRACE_SAMPLING_HEADER);
    }
    return this.clone({
      metadata: metadata.size === 0 ? undefined : metadata,
      samplingEnabled: enabled
    });
  }

  public metadata(): ReadonlyMap<string, string> {
    return this.#messageState.metadata ?? EMPTY_METADATA;
  }

  public withMetadata(metadata: ReadonlyMap<string, string>): MessageContext {
    const copy = new Map(metadata);
    const samplingEnabled =
      (copy.get(TRACE_SAMPLING_HEADER)?.length ?? 0) > 0 ||
      traceparentIsSampled(copy.get("traceparent"));
    return this.clone({ metadata: copy.size === 0 ? undefined : copy, samplingEnabled });
  }

  public streamId(): string | undefined {
    return this.#messageState.metadata?.get(STREAM_ID_HEADER);
  }

  public withStreamId(streamId: string): MessageContext {
    const metadata = new Map(this.metadata());
    metadata.set(STREAM_ID_HEADER, streamId);
    return this.clone({ metadata });
  }

  public priority(): number | undefined {
    return this.#messageState.priority;
  }

  public withPriority(priority: number): MessageContext {
    return this.clone({ priority });
  }

  public openTelemetryContext(): OpenTelemetryContext | undefined {
    return this.#messageState.openTelemetryContext;
  }

  public withOpenTelemetryContext(context: OpenTelemetryContext): MessageContext {
    return this.clone({ openTelemetryContext: context });
  }

  public transportMetadata(): ReadonlyMap<string, string> {
    const result = new Map<string, string>();
    for (const name of [
      STREAM_ID_HEADER,
      TRACE_SAMPLING_HEADER,
      "traceparent",
      "tracestate",
      "baggage"
    ]) {
      const value = this.#messageState.metadata?.get(name);
      if (value !== undefined) {
        result.set(name, value);
      }
    }
    return result;
  }

  /** @internal Attaches processing-side Activity state without serializing it. */
  public withDurableCallContext(durable: DurableCallExecutionContext): MessageContext {
    return this.clone({ durableCallContext: durable });
  }

  /** @internal Returns local state owned by the receiving Activity adapter. */
  public durableCallContext(): DurableCallExecutionContext | undefined {
    return this.#messageState.durableCallContext;
  }
}

function traceparentIsSampled(value: string | undefined): boolean {
  if (value === undefined) {
    return false;
  }
  const fields = value.split("-");
  const flags = fields[3];
  if (fields.length !== 4 || flags?.length !== 2) {
    return false;
  }
  const parsed = Number.parseInt(flags, 16);
  return Number.isFinite(parsed) && (parsed & 1) === 1;
}
