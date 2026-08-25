import {
  executeTemporalWorkflowEndpoint,
  makeScheduleTrigger,
  ScheduleBackend
} from "@gorundebug/tsservicelib/datasource/temporal/workflow";
import { makeInputStream, makeMapStream } from "@gorundebug/tsservicelib/operators";
import { makeDefaultSerdeRegistry } from "@gorundebug/tsservicelib/runtime/graph";

/** Bundle-only probe: both imports must remain valid inside the Workflow isolate. */
export function workflowSafeRuntimeProbe(): string {
  void executeTemporalWorkflowEndpoint;
  void makeDefaultSerdeRegistry;
  void makeInputStream;
  void makeMapStream;
  return makeScheduleTrigger(
    1,
    "bundle-probe",
    "2026-08-25T00:00:00.000Z",
    "2026-08-25T00:00:00.000Z",
    ScheduleBackend.Temporal
  ).triggerId;
}
