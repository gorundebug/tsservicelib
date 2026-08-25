import { type Context as TemporalActivityContext } from "@temporalio/activity";
import type { WorkflowClientInterceptor } from "@temporalio/client";
import type { ActivityInterceptors } from "@temporalio/worker";
import { MessageContext } from "../../runtime/context.js";
export declare const TEMPORAL_HEADER_DEADLINE_UNIX_NANO = "servicelib-deadline-unix-nano";
export declare const TEMPORAL_HEADER_PRIORITY = "servicelib-priority";
export declare function runWithTemporalSubmissionContext<T>(context: MessageContext, operation: () => Promise<T>): Promise<T>;
export declare function currentTemporalActivityMessageContext(): MessageContext;
export declare const temporalWorkflowClientInterceptor: WorkflowClientInterceptor;
export declare function temporalActivityInterceptors(temporalContext: TemporalActivityContext): ActivityInterceptors;
//# sourceMappingURL=context-propagation.d.ts.map