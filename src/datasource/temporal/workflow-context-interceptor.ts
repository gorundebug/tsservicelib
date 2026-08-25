import {
  type ActivityInput,
  type Headers,
  type Next,
  type WorkflowExecuteInput,
  type WorkflowInboundCallsInterceptor,
  type WorkflowInterceptors,
  type WorkflowOutboundCallsInterceptor
} from "@temporalio/workflow";

export function interceptors(): WorkflowInterceptors {
  let carrier: Headers = {};
  const inbound: WorkflowInboundCallsInterceptor = {
    execute(input: WorkflowExecuteInput, next): Promise<unknown> {
      carrier = input.headers;
      return next(input);
    }
  };
  const outbound: WorkflowOutboundCallsInterceptor = {
    scheduleActivity(
      input: ActivityInput,
      next: Next<WorkflowOutboundCallsInterceptor, "scheduleActivity">
    ): Promise<unknown> {
      return next({ ...input, headers: { ...carrier, ...input.headers } });
    }
  };
  return { inbound: [inbound], outbound: [outbound] };
}
