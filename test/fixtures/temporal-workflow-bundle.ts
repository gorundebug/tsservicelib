import {
  executeTemporalWorkflowEndpoint,
  makeScheduleTrigger,
  ScheduleBackend
} from "@gorundebug/tsservicelib/datasource/temporal/workflow";

/** Bundle-only probe: both imports must remain valid inside the Workflow isolate. */
export function workflowSafeRuntimeProbe(): string {
  void executeTemporalWorkflowEndpoint;
  return makeScheduleTrigger(
    1,
    "bundle-probe",
    "2026-08-25T00:00:00.000Z",
    "2026-08-25T00:00:00.000Z",
    ScheduleBackend.Temporal
  ).triggerId;
}
