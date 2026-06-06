import { ValidationRuntimeError } from "../errors";
import { emitRuntimeMetric, logRuntimeEvent } from "../logging";
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
  REALTIME_OUTBOX_LAG_METRIC,
  REALTIME_RUNTIME_EVICTIONS_METRIC,
} from "./types";

let nextRealtimeRuntimeId = 0;

export const REALTIME_CONFIGURED_RUNTIME_LIMIT = 1024;

export const configuredRealtimeRuntimes = new Map<string, RealtimeRuntime>();

export function configureRealtimeRuntime(runtime: RealtimeRuntime): string {
  nextRealtimeRuntimeId += 1;
  const runtimeId = `runtime:${nextRealtimeRuntimeId}`;
  configuredRealtimeRuntimes.set(runtimeId, runtime);
  trimConfiguredRealtimeRuntimes();
  return runtimeId;
}

export function resetRealtimeRuntimeStateForTest(): void {
  configuredRealtimeRuntimes.clear();
  nextRealtimeRuntimeId = 0;
}

function trimConfiguredRealtimeRuntimes(): void {
  while (configuredRealtimeRuntimes.size > REALTIME_CONFIGURED_RUNTIME_LIMIT) {
    const oldestRuntimeId = configuredRealtimeRuntimes.keys().next().value;
    if (typeof oldestRuntimeId !== "string") {
      return;
    }

    logRuntimeEvent("warn", "runtime.realtime_runtime_evicted", {
      limit: REALTIME_CONFIGURED_RUNTIME_LIMIT,
      runtimeId: oldestRuntimeId,
    });
    emitRealtimeMetric(REALTIME_RUNTIME_EVICTIONS_METRIC, 1, {
      result: "evicted",
    });
    configuredRealtimeRuntimes.delete(oldestRuntimeId);
  }
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

  return value;
}

export function getOptionalStringField(
  object: Record<string, unknown>,
  fieldName: string
): string | undefined {
  const value = object[fieldName];
  return typeof value === "string" && value.length > 0 ? value : undefined;
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
    typeof attachment.connectionName !== "string" ||
    typeof attachment.runtimeId !== "string"
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

  const subscriptions = Array.isArray(attachment.subscriptions)
    ? attachment.subscriptions.map(parseRealtimeSocketSubscription)
    : [];
  if (subscriptions.some((subscription) => subscription === null)) {
    return null;
  }

  return {
    authorizationHeader:
      typeof attachment.authorizationHeader === "string"
        ? attachment.authorizationHeader
        : undefined,
    connectionKey: attachment.connectionKey,
    connectionName: attachment.connectionName,
    latestDeliveredOutboxSequence:
      typeof latestDeliveredOutboxSequence === "number"
        ? latestDeliveredOutboxSequence
        : null,
    runtimeId: attachment.runtimeId,
    subscriptions: subscriptions as RealtimeSocketSubscription[],
  };
}

export function resolveRealtimeConnectionKey(url: URL): string {
  return (
    url.searchParams.get("clientId") ??
    url.searchParams.get("sessionId") ??
    `anonymous:${crypto.randomUUID()}`
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
