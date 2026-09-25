import { AsyncLocalStorage } from "node:async_hooks";

import { type Context as TemporalActivityContext } from "@temporalio/activity";
import type {
  WorkflowClientInterceptor,
  WorkflowStartInput,
  WorkflowStartOutput
} from "@temporalio/client";
import { defaultPayloadConverter, type Headers, type Next } from "@temporalio/common";
import type { ActivityInboundCallsInterceptor, ActivityInterceptors } from "@temporalio/worker";

import { MessageContext } from "../../runtime/context.js";
import { TEMPORAL_HEADER_DEADLINE_UNIX_NANO, TEMPORAL_HEADER_PRIORITY } from "./headers.js";

export { TEMPORAL_HEADER_DEADLINE_UNIX_NANO, TEMPORAL_HEADER_PRIORITY } from "./headers.js";

const CARRIER_NAMES = ["traceparent", "tracestate", "baggage", "x-trace", "x-stream-id"] as const;
const STREAM_ID_ONLY_NAMES = ["x-stream-id"] as const;
const TRACE_CARRIER_NAMES = new Set(["traceparent", "tracestate", "baggage", "x-trace"]);

const submissionContext = new AsyncLocalStorage<{
  context: MessageContext;
  tracingEnabled: boolean;
}>();
const activityMessageContext = new AsyncLocalStorage<MessageContext>();

export function runWithTemporalSubmissionContext<T>(
  context: MessageContext,
  operation: () => Promise<T>,
  tracingEnabled = true
): Promise<T> {
  return submissionContext.run({ context, tracingEnabled }, operation);
}

export function currentTemporalActivityMessageContext(): MessageContext {
  const context = activityMessageContext.getStore();
  if (context === undefined) {
    throw new Error("Temporal Activity MessageContext is not initialized");
  }
  return context;
}

export const temporalWorkflowClientInterceptor: WorkflowClientInterceptor = {
  async startWithDetails(
    input: WorkflowStartInput,
    next: Next<WorkflowClientInterceptor, "startWithDetails">
  ): Promise<WorkflowStartOutput> {
    const submission = submissionContext.getStore();
    if (submission === undefined) return next(input);
    const headers = submission.tracingEnabled
      ? input.headers
      : Object.fromEntries(
          Object.entries(input.headers).filter(([name]) => !TRACE_CARRIER_NAMES.has(name))
        );
    return next({
      ...input,
      headers: {
        ...headers,
        ...encodeContext(submission.context, submission.tracingEnabled)
      }
    });
  }
};

export function temporalActivityInterceptors(
  temporalContext: TemporalActivityContext,
  tracingEnabled = true
): ActivityInterceptors {
  const inbound: ActivityInboundCallsInterceptor = {
    async execute(input, next): Promise<unknown> {
      let context = decodeContext(input.headers, tracingEnabled).withExternalCancellation(
        temporalContext.cancellationSignal
      );
      if (temporalContext.info.startToCloseTimeoutMs > 0) {
        context = context.bounded(temporalContext.info.startToCloseTimeoutMs);
      }
      return activityMessageContext.run(context, () => next(input));
    }
  };
  return { inbound };
}

function encodeContext(context: MessageContext, tracingEnabled: boolean): Headers {
  const values = new Map(context.transportMetadata(tracingEnabled));
  const priority = context.priority();
  if (priority !== undefined) values.set(TEMPORAL_HEADER_PRIORITY, String(priority));
  const remainingMs = context.remainingMs();
  if (remainingMs !== undefined) {
    const unixMillis = BigInt(Date.now() + Math.max(0, Math.ceil(remainingMs)));
    values.set(TEMPORAL_HEADER_DEADLINE_UNIX_NANO, String(unixMillis * 1_000_000n));
  }
  return Object.fromEntries(
    [...values].map(([name, value]) => [name, defaultPayloadConverter.toPayload(value)])
  );
}

function decodeContext(headers: Headers, tracingEnabled: boolean): MessageContext {
  const metadata = new Map<string, string>();
  for (const name of tracingEnabled ? CARRIER_NAMES : STREAM_ID_ONLY_NAMES) {
    const value = decodeString(headers[name]);
    if (value !== undefined && value !== "") metadata.set(name, value);
  }
  let context = new MessageContext().withMetadata(metadata);
  const priority = Number.parseInt(decodeString(headers[TEMPORAL_HEADER_PRIORITY]) ?? "", 10);
  if (Number.isSafeInteger(priority)) context = context.withPriority(priority);
  const deadline = decodeString(headers[TEMPORAL_HEADER_DEADLINE_UNIX_NANO]);
  if (deadline !== undefined) {
    try {
      const deadlineUnixMillis = Number(BigInt(deadline) / 1_000_000n);
      if (Number.isSafeInteger(deadlineUnixMillis)) {
        context = context.bounded(Math.max(0, deadlineUnixMillis - Date.now()));
      }
    } catch {
      // Invalid external carrier fields are ignored consistently with HTTP/Kafka metadata.
    }
  }
  return context;
}

function decodeString(payload: Headers[string] | undefined): string | undefined {
  if (payload === undefined) return undefined;
  try {
    const value = defaultPayloadConverter.fromPayload<unknown>(payload);
    return typeof value === "string" ? value : undefined;
  } catch {
    return undefined;
  }
}
