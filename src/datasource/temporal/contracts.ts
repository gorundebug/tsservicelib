export const ENDPOINT_WORKFLOW_TYPE = "servicelib.temporal-endpoint.v1";

export function temporalIdentityName(value: string): string {
  const words: string[] = [];
  let current: string[] = [];
  const characters = Array.from(value);
  for (const [index, character] of characters.entries()) {
    if (/\s/u.test(character) || ["_", "-", "/", "."].includes(character)) {
      if (current.length > 0) {
        words.push(current.join(""));
        current = [];
      }
      continue;
    }
    if (!/[\p{L}\p{N}]/u.test(character)) continue;
    const upper = character.toUpperCase() === character && character.toLowerCase() !== character;
    if (current.length > 0 && upper) {
      const previous = current.at(-1) ?? "";
      const previousUpper =
        previous.toUpperCase() === previous && previous.toLowerCase() !== previous;
      const next = characters[index + 1];
      const nextLower = next?.toLowerCase() === next && next?.toUpperCase() !== next;
      if (!previousUpper || nextLower) {
        words.push(current.join(""));
        current = [];
      }
    }
    current.push(character);
  }
  if (current.length > 0) words.push(current.join(""));
  return words.map((word) => word.toLowerCase()).join("_");
}

export function temporalEndpointActivityType(connectorName: string, endpointName: string): string {
  return `${temporalIdentityName(connectorName)}.endpoint.${temporalIdentityName(endpointName)}.v1`;
}

export function temporalDirectWorkflowType(connectorName: string, endpointName: string): string {
  return `${temporalIdentityName(connectorName)}.endpoint.${temporalIdentityName(endpointName)}.workflow.v1`;
}

export function temporalEndpointWorkflowId(
  connectorName: string,
  endpointName: string,
  messageId: string
): string {
  const opaque = encodeURIComponent(messageId).replaceAll(
    /[!'()*]/g,
    (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`
  );
  return `${temporalIdentityName(connectorName)}/endpoint/${temporalIdentityName(endpointName)}/${opaque}`;
}

export interface EndpointEnvelope {
  readonly version: number;
  readonly endpointId: number;
  readonly messageId: string;
  readonly streamId: string;
  readonly priority: number;
  readonly deadlineUnixMillis: number;
  readonly scheduled: boolean;
  readonly scheduleId: string;
  readonly scheduledAtUnixMillis: number;
  readonly firedAtUnixMillis: number;
  readonly payload: Uint8Array;
}

export interface EndpointResult {
  readonly payload: Uint8Array;
}

export interface EndpointWireEnvelope extends Omit<EndpointEnvelope, "payload"> {
  readonly payload: readonly number[];
}

export interface EndpointWireResult {
  readonly payload: readonly number[];
}

export interface EndpointWorkflowRequest {
  readonly executionType: TemporalExecutionType;
  readonly runtimeConfig: CanonicalConfig;
  readonly activityType: string;
  readonly activityStartToCloseTimeout: number;
  readonly activityHeartbeatTimeout: number;
  readonly maximumAttempts: number;
  readonly priority: number;
  readonly envelope: EndpointWireEnvelope;
}
import type { CanonicalConfig, TemporalExecutionType } from "../../runtime/config/types.js";
