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

const submissionContext = new AsyncLocalStorage<MessageContext>();
const activityMessageContext = new AsyncLocalStorage<MessageContext>();

export function runWithTemporalSubmissionContext<T>(
  context: MessageContext,
  operation: () => Promise<T>
): Promise<T> {
  return submissionContext.run(context, operation);
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
    const context = submissionContext.getStore();
    if (context === undefined) return next(input);
    return next({ ...input, headers: { ...input.headers, ...encodeContext(context) } });
  }
};

export function temporalActivityInterceptors(
  temporalContext: TemporalActivityContext
): ActivityInterceptors {
  const inbound: ActivityInboundCallsInterceptor = {
    async execute(input, next): Promise<unknown> {
      let context = decodeContext(input.headers).withExternalCancellation(
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

function encodeContext(context: MessageContext): Headers {
  const values = new Map(context.transportMetadata());
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

function decodeContext(headers: Headers): MessageContext {
  const metadata = new Map<string, string>();
  for (const name of CARRIER_NAMES) {
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
