import { executeChild, scheduleActivity, workflowInfo } from "@temporalio/workflow";

import {
  isTemporalEndpointConfig
} from "../../runtime/config/schedule.js";
import type {
  CanonicalConfig,
  TemporalEndpointConfig
} from "../../runtime/config/types.js";
import type { Consumer } from "../../runtime/stream.js";
import type { TypedSinkStream, TypedSinkStreamWithResult } from "../../runtime/data-sink.js";
import { normalizeTemporalPriority } from "../../runtime/schedule.js";
import {
  temporalDirectWorkflowType,
  temporalEndpointActivityType,
  temporalEndpointWorkflowId,
  temporalIdentityName,
  type EndpointWireEnvelope,
  type EndpointWireResult,
  type EndpointWorkflowRequest
} from "./contracts.js";

/** Attach a plain Temporal sink to a graph executing inside a Workflow isolate. */
export function makeTemporalWorkflowSinkEndpointConsumer<T, E>(
  stream: TypedSinkStream<T, E>
): Consumer<T> {
  return makeWorkflowSinkConsumer<T, never, E>(stream, false);
}

/** Attach a result-producing Temporal sink to a Workflow graph. */
export function makeTemporalWorkflowSinkEndpointConsumerWithResult<T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>
): Consumer<T> {
  return makeWorkflowSinkConsumer(stream, true);
}

function makeWorkflowSinkConsumer<T, R, E>(
  stream: TypedSinkStream<T, E> | TypedSinkStreamWithResult<T, R, E>,
  withResult: boolean
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  const endpoint = environment.runtimeConfig().endpointById(stream.endpointId());
  if (!isTemporalEndpointConfig(endpoint)) {
    throw new Error(`Temporal endpoint config ${String(stream.endpointId())} not found`);
  }
  const connector = environment.runtimeConfig().dataConnectorById(endpoint.idDataConnector);
  if (connector === undefined) {
    throw new Error(`Temporal connector ${String(endpoint.idDataConnector)} not found`);
  }
  const consumer: Consumer<T> = {
    consume: async (context, value) => {
      const parentId = context.streamId() ?? workflowInfo().workflowId;
      const messageId = `${parentId}/${temporalIdentityName(stream.name)}`;
      const remainingMs = context.remainingMs();
      const envelope: EndpointWireEnvelope = {
        version: 1,
        endpointId: endpoint.id,
        messageId,
        streamId: parentId,
        priority: context.priority() ?? 0,
        deadlineUnixMillis:
          remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
        scheduled: false,
        scheduleId: "",
        scheduledAtUnixMillis: 0,
        firedAtUnixMillis: 0,
        payload: Array.from(stream.inputSerde().serialize(value))
      };
      const result = await executeEndpoint(
        endpoint,
        connector.name,
        environment.runtimeConfig().config(),
        envelope
      );
      if (withResult) {
        const resultStream = stream as TypedSinkStreamWithResult<T, R, E>;
        await resultStream.consumeResult(
          context,
          resultStream.serde().deserialize(Uint8Array.from(result.payload))
        );
      }
    }
  };
  stream.setSinkConsumer(consumer);
  return consumer;
}

async function executeEndpoint(
  endpoint: TemporalEndpointConfig,
  connectorName: string,
  runtimeConfig: CanonicalConfig,
  envelope: EndpointWireEnvelope
): Promise<EndpointWireResult> {
  const request: EndpointWorkflowRequest = {
    executionType: endpoint.temporalExecutionType,
    runtimeConfig,
    activityType: temporalEndpointActivityType(connectorName, endpoint.name),
    activityStartToCloseTimeout: endpoint.activityStartToCloseTimeout,
    activityHeartbeatTimeout: endpoint.activityHeartbeatTimeout,
    maximumAttempts: endpoint.maximumAttempts,
    priority: normalizeTemporalPriority(envelope.priority),
    envelope
  };
  if (endpoint.temporalExecutionType === "Activity") {
    return scheduleActivity<EndpointWireResult>(request.activityType, [envelope], {
      taskQueue: endpoint.taskQueue,
      startToCloseTimeout: endpoint.activityStartToCloseTimeout,
      ...(endpoint.activityHeartbeatTimeout > 0
        ? { heartbeatTimeout: endpoint.activityHeartbeatTimeout }
        : {}),
      retry: { maximumAttempts: endpoint.maximumAttempts },
      priority: { priorityKey: request.priority }
    });
  }
  return executeChild<(request: EndpointWorkflowRequest) => Promise<EndpointWireResult>>(
    temporalDirectWorkflowType(connectorName, endpoint.name),
    {
      args: [request],
      workflowId: temporalEndpointWorkflowId(connectorName, endpoint.name, envelope.messageId),
      taskQueue: endpoint.taskQueue,
      ...(endpoint.workflowExecutionTimeout > 0
        ? { workflowExecutionTimeout: endpoint.workflowExecutionTimeout }
        : {})
    }
  );
}
