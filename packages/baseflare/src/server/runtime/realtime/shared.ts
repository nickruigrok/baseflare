import { InternalRuntimeError, ValidationRuntimeError } from "../errors";
import { emitRuntimeMetric, logRuntimeEvent } from "../logging";
import { sha256Hex } from "./hash";
import { getRealtimeConnectionShardName } from "./routing";
import type {
  PendingRealtimeDelivery,
  RealtimeDurableObjectState,
  RealtimeMetricResult,
  RealtimeMetricSource,
  RealtimeRuntime,
  RealtimeSequencedOutboxEvent,
  RealtimeSocketAttachment,
  RealtimeSocketSubscription,
} from "./types";
import {
  JSON_HEADERS,
  REALTIME_DELIVERY_BATCH_SIZE,
  REALTIME_MAX_IDENTIFIER_LENGTH,
  REALTIME_MAX_JSON_DEPTH,
  REALTIME_MAX_JSON_NODES,
  REALTIME_MAX_JSON_STRING_LENGTH,
  REALTIME_OUTBOX_LAG_METRIC,
  REALTIME_RUNTIME_LIMIT_EXCEEDED_METRIC,
} from "./types";

let nextRealtimeRuntimeId = 0;

export const REALTIME_CONFIGURED_RUNTIME_LIMIT = 1024;

export const configuredRealtimeRuntimes = new Map<string, RealtimeRuntime>();

export function configureRealtimeRuntime(runtime: RealtimeRuntime): string {
  if (configuredRealtimeRuntimes.size >= REALTIME_CONFIGURED_RUNTIME_LIMIT) {
    logRuntimeEvent("warn", "runtime.realtime_runtime_limit_exceeded", {
      limit: REALTIME_CONFIGURED_RUNTIME_LIMIT,
      size: configuredRealtimeRuntimes.size,
    });
    emitRealtimeMetric(REALTIME_RUNTIME_LIMIT_EXCEEDED_METRIC, 1, {
      result: "limit_exceeded",
    });
    throw new InternalRuntimeError(
      `Realtime runtime configuration limit exceeded: ${REALTIME_CONFIGURED_RUNTIME_LIMIT}`
    );
  }

  nextRealtimeRuntimeId += 1;
  const runtimeId = `runtime:${nextRealtimeRuntimeId}`;
  configuredRealtimeRuntimes.set(runtimeId, runtime);
  return runtimeId;
}

export function resetRealtimeRuntimeStateForTest(): void {
  configuredRealtimeRuntimes.clear();
  nextRealtimeRuntimeId = 0;
}

export function jsonResponse(
  value: unknown,
  init: ResponseInit = {}
): Response {
  return Response.json(value, {
    ...init,
    headers: { ...JSON_HEADERS, ...init.headers },
  });
}

export function parseObject(
  value: unknown,
  label: string
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new ValidationRuntimeError(`${label} must be an object`);
  }

  return value as Record<string, unknown>;
}

export async function readJsonObject(
  request: Request
): Promise<Record<string, unknown>> {
  try {
    return parseObject((await request.json()) as unknown, "Realtime message");
  } catch (error) {
    if (error instanceof ValidationRuntimeError) {
      throw error;
    }

    throw new ValidationRuntimeError("Realtime message JSON is malformed");
  }
}

export function getStringField(
  object: Record<string, unknown>,
  fieldName: string
): string {
  const value = object[fieldName];
  if (typeof value !== "string" || value.length === 0) {
    throw new ValidationRuntimeError(
      `Realtime field "${fieldName}" must be a non-empty string`
    );
  }
  assertRealtimeStringLength(value, fieldName, REALTIME_MAX_IDENTIFIER_LENGTH);

  return value;
}

export function getOptionalStringField(
  object: Record<string, unknown>,
  fieldName: string
): string | undefined {
  const value = object[fieldName];
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

export function assertRealtimeStringLength(
  value: string,
  fieldName: string,
  maxLength: number
): void {
  if (value.length > maxLength) {
    throw new ValidationRuntimeError(
      `Realtime field "${fieldName}" must be at most ${maxLength} characters`
    );
  }
}

export function assertRealtimeJsonBounds(value: unknown, label: string): void {
  let nodeCount = 0;
  const stack: Array<{ readonly depth: number; readonly value: unknown }> = [
    { depth: 0, value },
  ];

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) {
      continue;
    }

    nodeCount += 1;
    if (nodeCount > REALTIME_MAX_JSON_NODES) {
      throw new ValidationRuntimeError(
        `${label} must contain at most ${REALTIME_MAX_JSON_NODES} JSON nodes`
      );
    }

    if (current.depth > REALTIME_MAX_JSON_DEPTH) {
      throw new ValidationRuntimeError(
        `${label} must be at most ${REALTIME_MAX_JSON_DEPTH} levels deep`
      );
    }

    pushRealtimeJsonChildren(current.value, current.depth, label, stack);
  }
}

function pushRealtimeJsonChildren(
  value: unknown,
  depth: number,
  label: string,
  stack: Array<{ readonly depth: number; readonly value: unknown }>
): void {
  if (typeof value === "string") {
    assertRealtimeStringLength(value, label, REALTIME_MAX_JSON_STRING_LENGTH);
    return;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      stack.push({ depth: depth + 1, value: item });
    }
    return;
  }

  if (typeof value !== "object" || value === null) {
    return;
  }

  const prototype = Object.getPrototypeOf(value);
  if (!(prototype === Object.prototype || prototype === null)) {
    throw new ValidationRuntimeError(`${label} must be JSON-serializable`);
  }

  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    assertRealtimeStringLength(
      key,
      `${label} key`,
      REALTIME_MAX_JSON_STRING_LENGTH
    );
    stack.push({ depth: depth + 1, value: child });
  }
}

export function getEpoch(value: unknown): number {
  if (value === undefined) {
    return 0;
  }

  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new ValidationRuntimeError(
      'Realtime field "epoch" must be a non-negative integer'
    );
  }

  return value;
}

export function getOptionalSequence(
  value: unknown,
  fieldName: string
): number | null {
  if (value === undefined || value === null) {
    return null;
  }

  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new ValidationRuntimeError(
      `Realtime field "${fieldName}" must be a non-negative integer or null`
    );
  }

  return value as number;
}

export function isRealtimeDurableObjectState(
  value: unknown
): value is RealtimeDurableObjectState {
  return typeof value === "object" && value !== null;
}

export function createRealtimeSocketAttachment(input: {
  readonly authorizationHeader: string | null | undefined;
  readonly connectionKey: string;
  readonly latestDeliveredOutboxSequence?: number | null;
  readonly runtimeId: string;
  readonly subscriptions?: readonly RealtimeSocketSubscription[];
}): RealtimeSocketAttachment {
  return {
    authorizationHeader: input.authorizationHeader ?? undefined,
    connectionKey: input.connectionKey,
    connectionName: getRealtimeConnectionShardName(input.connectionKey),
    latestDeliveredOutboxSequence: input.latestDeliveredOutboxSequence ?? null,
    runtimeId: input.runtimeId,
    subscriptions: input.subscriptions ?? [],
  };
}

function parseRealtimeSocketSubscription(
  value: unknown
): RealtimeSocketSubscription | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }

  const subscription = value as Record<string, unknown>;
  if (
    typeof subscription.queryName !== "string" ||
    typeof subscription.subscriptionId !== "string" ||
    typeof subscription.epoch !== "number" ||
    !Number.isInteger(subscription.epoch) ||
    subscription.epoch < 0
  ) {
    return null;
  }

  return {
    args: subscription.args ?? {},
    epoch: subscription.epoch,
    queryName: subscription.queryName,
    subscriptionShardName:
      typeof subscription.subscriptionShardName === "string"
        ? subscription.subscriptionShardName
        : undefined,
    subscriptionId: subscription.subscriptionId,
  };
}

export function parseRealtimeSocketAttachment(
  value: unknown
): RealtimeSocketAttachment | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }

  const attachment = value as Record<string, unknown>;
  if (
    typeof attachment.connectionKey !== "string" ||
    attachment.connectionKey.length === 0 ||
    typeof attachment.runtimeId !== "string" ||
    attachment.runtimeId.length === 0
  ) {
    return null;
  }

  const latestDeliveredOutboxSequence =
    attachment.latestDeliveredOutboxSequence;
  if (
    latestDeliveredOutboxSequence !== null &&
    latestDeliveredOutboxSequence !== undefined &&
    !Number.isSafeInteger(latestDeliveredOutboxSequence)
  ) {
    return null;
  }

  const subscriptions: RealtimeSocketSubscription[] = [];
  if (Array.isArray(attachment.subscriptions)) {
    for (const [
      subscriptionIndex,
      subscription,
    ] of attachment.subscriptions.entries()) {
      const parsedSubscription = parseRealtimeSocketSubscription(subscription);
      if (parsedSubscription) {
        subscriptions.push(parsedSubscription);
        continue;
      }

      logRuntimeEvent(
        "warn",
        "runtime.realtime_socket_subscription_attachment_dropped",
        {
          connectionKey: attachment.connectionKey,
          runtimeId: attachment.runtimeId,
          subscriptionIndex,
        }
      );
    }
  }

  return {
    authorizationHeader:
      typeof attachment.authorizationHeader === "string"
        ? attachment.authorizationHeader
        : undefined,
    connectionKey: attachment.connectionKey,
    connectionName: getRealtimeConnectionShardName(attachment.connectionKey),
    latestDeliveredOutboxSequence:
      typeof latestDeliveredOutboxSequence === "number"
        ? latestDeliveredOutboxSequence
        : null,
    runtimeId: attachment.runtimeId,
    subscriptions,
  };
}

export async function resolveRealtimeConnectionKey(
  url: URL,
  input: {
    readonly authorizationHeader?: string | null;
    readonly runtimeId: string;
  }
): Promise<string> {
  const clientId = url.searchParams.get("clientId");
  if (clientId && input.authorizationHeader) {
    assertRealtimeStringLength(
      clientId,
      "clientId",
      REALTIME_MAX_IDENTIFIER_LENGTH
    );
    return `client:${await createAuthBoundConnectionKeyHash(
      input.runtimeId,
      input.authorizationHeader,
      clientId
    )}`;
  }

  const sessionId = url.searchParams.get("sessionId");
  if (sessionId && input.authorizationHeader) {
    assertRealtimeStringLength(
      sessionId,
      "sessionId",
      REALTIME_MAX_IDENTIFIER_LENGTH
    );
    return `session:${await createAuthBoundConnectionKeyHash(
      input.runtimeId,
      input.authorizationHeader,
      sessionId
    )}`;
  }

  return `anonymous:${crypto.randomUUID()}`;
}

async function createAuthBoundConnectionKeyHash(
  runtimeId: string,
  authorizationHeader: string,
  explicitId: string
): Promise<string> {
  const authFingerprint = await sha256Hex(authorizationHeader);
  return await sha256Hex(
    JSON.stringify([runtimeId, authFingerprint, explicitId])
  );
}

export function emitRealtimeMetric(
  name: string,
  value: number,
  tags: {
    readonly result?: RealtimeMetricResult;
    readonly source?: RealtimeMetricSource;
  }
): void {
  if (value <= 0) {
    return;
  }

  const metricTags: Record<string, string> = {};
  if (tags.result) {
    metricTags.result = tags.result;
  }
  if (tags.source) {
    metricTags.source = tags.source;
  }

  emitRuntimeMetric(name, value, metricTags);
}

export function emitOutboxLagMetric(
  source: RealtimeMetricSource,
  events: readonly RealtimeSequencedOutboxEvent[]
): number {
  const now = Date.now();
  const lagMs = events.reduce(
    (maxLagMs, event) => Math.max(maxLagMs, now - event.createdAt),
    0
  );
  emitRealtimeMetric(REALTIME_OUTBOX_LAG_METRIC, lagMs, { source });
  return lagMs;
}

export function chunkRealtimeDeliveries(
  deliveries: readonly PendingRealtimeDelivery[]
): PendingRealtimeDelivery[][] {
  const chunks: PendingRealtimeDelivery[][] = [];
  for (
    let index = 0;
    index < deliveries.length;
    index += REALTIME_DELIVERY_BATCH_SIZE
  ) {
    chunks.push(deliveries.slice(index, index + REALTIME_DELIVERY_BATCH_SIZE));
  }

  return chunks;
}
