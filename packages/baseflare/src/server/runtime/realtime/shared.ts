import { InternalRuntimeError, ValidationRuntimeError } from "../errors";
import { assertJsonBounds } from "../json-bounds";
import { emitRuntimeMetric, logRuntimeEvent } from "../logging";
import { sha256Hex } from "./hash";
import {
  getRealtimeConnectionShardName,
  parseRealtimeSubscriptionShardName,
} from "./routing";
import type {
  PendingRealtimeDelivery,
  RealtimeDurableObjectState,
  RealtimeMetricResult,
  RealtimeMetricSource,
  RealtimeRegistration,
  RealtimeRuntime,
  RealtimeSequencedOutboxEvent,
  RealtimeSocketAttachment,
  RealtimeSocketSubscription,
} from "./types";
import {
  JSON_HEADERS,
  REALTIME_DELIVERY_BATCH_MAX_BYTES,
  REALTIME_DELIVERY_BATCH_SIZE,
  REALTIME_LEASE_MS,
  REALTIME_MAX_ACTIVE_SUBSCRIPTIONS_PER_SOCKET,
  REALTIME_MAX_IDENTIFIER_LENGTH,
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

/** @internal test-only */
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
  readonly authorizationFingerprint?: string;
  readonly connectionKey: string;
  readonly latestDeliveredOutboxSequence?: number | null;
  readonly runtimeId: string;
  readonly subscriptions?: readonly RealtimeSocketSubscription[];
}): RealtimeSocketAttachment {
  return {
    authorizationFingerprint: input.authorizationFingerprint,
    connectionKey: input.connectionKey,
    connectionName: getRealtimeConnectionShardName(input.connectionKey),
    latestDeliveredOutboxSequence: input.latestDeliveredOutboxSequence ?? null,
    runtimeId: input.runtimeId,
    subscriptions: input.subscriptions ?? [],
  };
}

function parseRealtimeSocketSubscription(
  value: unknown,
  context: {
    readonly connectionKey: string;
    readonly runtimeId: string;
    readonly subscriptionIndex: number;
  }
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

  const args = subscription.args ?? {};
  try {
    assertRealtimeStringLength(
      subscription.queryName,
      "queryName",
      REALTIME_MAX_IDENTIFIER_LENGTH
    );
    assertRealtimeStringLength(
      subscription.subscriptionId,
      "subscriptionId",
      REALTIME_MAX_IDENTIFIER_LENGTH
    );
    assertJsonBounds(args, "Realtime subscription args");
  } catch {
    return null;
  }

  const subscriptionShardName = parseStoredSubscriptionShardName(
    subscription.subscriptionShardName,
    context
  );

  return {
    args,
    epoch: subscription.epoch,
    queryName: subscription.queryName,
    subscriptionShardName,
    subscriptionId: subscription.subscriptionId,
  };
}

function parseStoredSubscriptionShardName(
  value: unknown,
  context: {
    readonly connectionKey: string;
    readonly runtimeId: string;
    readonly subscriptionIndex: number;
  }
): string | undefined {
  if (typeof value !== "string" || value.length === 0) {
    return undefined;
  }

  try {
    assertRealtimeStringLength(
      value,
      "subscriptionShardName",
      REALTIME_MAX_IDENTIFIER_LENGTH
    );
  } catch {
    return undefined;
  }

  if (parseRealtimeSubscriptionShardName(value)) {
    return value;
  }

  logRuntimeEvent(
    "warn",
    "runtime.realtime_socket_subscription_shard_cleared",
    {
      connectionKey: context.connectionKey,
      runtimeId: context.runtimeId,
      subscriptionIndex: context.subscriptionIndex,
    }
  );
  return undefined;
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
  try {
    assertRealtimeStringLength(
      attachment.connectionKey,
      "connectionKey",
      REALTIME_MAX_IDENTIFIER_LENGTH
    );
    assertRealtimeStringLength(
      attachment.runtimeId,
      "runtimeId",
      REALTIME_MAX_IDENTIFIER_LENGTH
    );
  } catch {
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
    if (
      attachment.subscriptions.length >
      REALTIME_MAX_ACTIVE_SUBSCRIPTIONS_PER_SOCKET
    ) {
      logRuntimeEvent(
        "warn",
        "runtime.realtime_socket_subscription_attachment_dropped",
        {
          connectionKey: attachment.connectionKey,
          runtimeId: attachment.runtimeId,
          subscriptionIndex: REALTIME_MAX_ACTIVE_SUBSCRIPTIONS_PER_SOCKET,
        }
      );
    }

    for (const [subscriptionIndex, subscription] of attachment.subscriptions
      .slice(0, REALTIME_MAX_ACTIVE_SUBSCRIPTIONS_PER_SOCKET)
      .entries()) {
      const parsedSubscription = parseRealtimeSocketSubscription(subscription, {
        connectionKey: attachment.connectionKey,
        runtimeId: attachment.runtimeId,
        subscriptionIndex,
      });
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
    authorizationFingerprint:
      typeof attachment.authorizationFingerprint === "string"
        ? attachment.authorizationFingerprint
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
  if (clientId) {
    if (!input.authorizationHeader) {
      throw new ValidationRuntimeError(
        "Realtime clientId requires an authorization header to produce a stable connection key"
      );
    }
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
  if (sessionId) {
    if (!input.authorizationHeader) {
      throw new ValidationRuntimeError(
        "Realtime sessionId requires an authorization header to produce a stable connection key"
      );
    }
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

export async function createRealtimeAuthorizationFingerprint(
  authorizationHeader: string
): Promise<string> {
  return await sha256Hex(authorizationHeader);
}

export async function parseRealtimeRegistration(
  body: Record<string, unknown>
): Promise<RealtimeRegistration> {
  return {
    args: body.args ?? {},
    authorizationFingerprint: await parseAuthorizationFingerprint(body),
    connectionKey: getStringField(body, "connectionKey"),
    connectionName: getStringField(body, "connectionName"),
    epoch: getEpoch(body.epoch),
    leaseExpiresAt:
      typeof body.leaseExpiresAt === "number"
        ? body.leaseExpiresAt
        : Date.now() + REALTIME_LEASE_MS,
    queryName: getStringField(body, "queryName"),
    runtimeId: getStringField(body, "runtimeId"),
    subscriptionId: getStringField(body, "subscriptionId"),
  };
}

async function parseAuthorizationFingerprint(
  body: Record<string, unknown>
): Promise<string | undefined> {
  if (typeof body.authorizationFingerprint === "string") {
    return body.authorizationFingerprint;
  }

  if (typeof body.authorizationHeader === "string") {
    return await createRealtimeAuthorizationFingerprint(
      body.authorizationHeader
    );
  }

  return undefined;
}

async function createAuthBoundConnectionKeyHash(
  runtimeId: string,
  authorizationHeader: string,
  explicitId: string
): Promise<string> {
  const authFingerprint =
    await createRealtimeAuthorizationFingerprint(authorizationHeader);
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

/**
 * Runs `work` over `items` with at most `limit` items in flight. Callers own
 * error handling inside `work`; a thrown error stops that worker's loop.
 */
export async function runWithConcurrency<T>(
  items: readonly T[],
  limit: number,
  work: (item: T) => Promise<void>
): Promise<void> {
  let nextIndex = 0;
  const worker = async (): Promise<void> => {
    while (nextIndex < items.length) {
      const item = items[nextIndex];
      nextIndex += 1;
      if (item !== undefined) {
        await work(item);
      }
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, () => worker())
  );
}

/**
 * Splits deliveries into /deliver batches bounded by item count AND by
 * cumulative result size, so a burst of near-cap results never serializes a
 * batch body large enough to threaten the 128 MB isolate memory limit. A
 * single delivery always ships, alone if necessary — its result is already
 * bounded by REALTIME_MAX_RESULT_JSON_BYTES.
 */
export function chunkRealtimeDeliveries(
  deliveries: readonly PendingRealtimeDelivery[]
): PendingRealtimeDelivery[][] {
  const chunks: PendingRealtimeDelivery[][] = [];
  let chunk: PendingRealtimeDelivery[] = [];
  let chunkBytes = 0;
  for (const delivery of deliveries) {
    const deliveryBytes = delivery.resultJson.length;
    const exceedsBytes =
      chunkBytes + deliveryBytes > REALTIME_DELIVERY_BATCH_MAX_BYTES;
    if (
      chunk.length > 0 &&
      (chunk.length >= REALTIME_DELIVERY_BATCH_SIZE || exceedsBytes)
    ) {
      chunks.push(chunk);
      chunk = [];
      chunkBytes = 0;
    }

    chunk.push(delivery);
    chunkBytes += deliveryBytes;
  }

  if (chunk.length > 0) {
    chunks.push(chunk);
  }

  return chunks;
}
