import type { Rules } from "../../permissions/types";
import type { Schema } from "../../schema/types";
import type { FunctionIndex } from "../function-index";
import type { BaseflareRuntimeEnv, DurableObjectNamespace } from "../types";

export type RuntimeWebSocket = WebSocket & {
  accept(): void;
  deserializeAttachment?(): unknown;
  serializeAttachment?(attachment: unknown): void;
};

export interface RealtimeStorage {
  delete(keys: readonly string[] | string): Promise<void>;
  deleteAlarm(): Promise<void>;
  get<T = unknown>(key: string): Promise<T | undefined>;
  getAlarm(): Promise<number | null>;
  list<T = unknown>(options?: {
    readonly limit?: number;
    readonly prefix?: string;
    readonly startAfter?: string;
  }): Promise<Map<string, T>>;
  put<T = unknown>(
    keyOrEntries: Record<string, T> | string,
    value?: T
  ): Promise<void>;
  setAlarm(scheduledTime: number): Promise<void>;
}

// The production Durable Object runtime always provides storage and the
// Hibernation API; both are optional only for lightweight in-memory test
// states that run DOs without persistence.
export interface RealtimeDurableObjectState {
  acceptWebSocket?(socket: RuntimeWebSocket): void;
  getWebSockets?(): RuntimeWebSocket[];
  storage?: RealtimeStorage;
}

export interface RealtimePartitionTarget {
  readonly partitionKey: string;
  readonly partitionValue: string;
  readonly tableName: string;
}

export interface RealtimeOutboxEvent {
  readonly eventId: string;
  readonly partitions: readonly RealtimePartitionTarget[];
  readonly tables: readonly string[];
}

export interface RealtimeSequencedOutboxEvent extends RealtimeOutboxEvent {
  readonly createdAt: number;
  readonly sequence: number;
}

export interface RealtimeMutationNotifier {
  readonly enabled: true;
  notify(
    events: readonly RealtimeOutboxEvent[],
    options?: { readonly outboxBookmark?: string | null }
  ): void;
}

export interface RealtimeOutboxOperation {
  readonly event: RealtimeOutboxEvent;
  readonly expectedChanges: number;
  readonly params: readonly (string | number | null)[];
  readonly sql: string;
  readonly type: "insert-realtime-outbox";
}

export interface RealtimeRegistration {
  readonly args: unknown;
  readonly authorizationFingerprint?: string;
  readonly connectionKey: string;
  readonly connectionName: string;
  readonly epoch: number;
  readonly leaseExpiresAt: number;
  readonly queryName: string;
  readonly runtimeId: string;
  readonly subscriptionId: string;
}

export interface RealtimeObjectEnv extends BaseflareRuntimeEnv {
  REALTIME_CONNECTIONS: DurableObjectNamespace;
  REALTIME_SUBSCRIPTIONS: DurableObjectNamespace;
}

export interface RealtimeRuntime {
  readonly functionIndex: FunctionIndex;
  readonly rules?: Rules;
  readonly schema: Schema;
}

export type StoredRealtimeRegistration = Omit<
  RealtimeRegistration,
  "leaseExpiresAt"
> & {
  activeQueryKey?: string;
  dependencies?: RealtimeDependencySet;
  leaseExpiresAt: number;
  lastResultJson?: string;
  movePending?: boolean;
  reEvaluationRetryAt?: number;
  versionSnapshot?: RealtimeVersionSnapshot;
};

export interface StoredRealtimeActiveQuery {
  readonly args: unknown;
  readonly authorizationFingerprint?: string;
  dependencies?: RealtimeDependencySet;
  readonly key: string;
  lastResultJson?: string;
  readonly memberRegistrationKeys: Set<string>;
  readonly queryName: string;
  reEvaluationRetryAt?: number;
  readonly runtimeId: string;
  versionSnapshot?: RealtimeVersionSnapshot;
}

export interface RealtimeDependencySet {
  readonly partitions: ReadonlySet<string>;
  readonly tables: ReadonlySet<string>;
}

export interface RealtimeVersionSnapshot {
  readonly partitions: ReadonlyMap<string, number>;
  readonly tables: ReadonlyMap<string, number>;
}

export interface RealtimePressureSnapshot {
  readonly activeQueryCount?: number;
  readonly activeRegistrationCount: number;
  readonly deliveryBatchLatencyMs?: number;
  readonly failedDeliveryRate?: number;
  readonly maxFanoutPerActiveQuery?: number;
  readonly outboxLagMs?: number;
  readonly pendingWorkCount?: number;
  readonly reEvaluationLatencyMs?: number;
}

export interface RealtimeAffectedTargets {
  readonly all: boolean;
  readonly broadTables: ReadonlySet<string>;
  readonly partitions: ReadonlySet<string>;
  readonly sequence: number | null;
  readonly tables: ReadonlySet<string>;
}

export type RealtimeSubscriptionRouteTarget =
  | {
      readonly type: "global";
    }
  | {
      readonly tableName: string;
      readonly type: "table";
    }
  | {
      readonly partition: RealtimePartitionTarget;
      readonly type: "partition";
    };

export type RealtimeShardGenerationStatus = "active" | "draining" | "retired";

export interface RealtimeShardGeneration {
  readonly createdAt: number;
  readonly drainAfter: number | null;
  readonly generationId: number;
  readonly status: RealtimeShardGenerationStatus;
  readonly subscriptionShardCount: number;
}

export interface RealtimeSocketSubscription {
  readonly args: unknown;
  readonly epoch: number;
  readonly queryName: string;
  readonly subscriptionId: string;
  readonly subscriptionShardName?: string;
}

export interface RealtimeSocketAttachment {
  readonly authorizationFingerprint?: string;
  readonly connectionKey: string;
  readonly connectionName: string;
  readonly latestDeliveredOutboxSequence: number | null;
  readonly runtimeId: string;
  readonly subscriptions: readonly RealtimeSocketSubscription[];
}

export interface RealtimeSocketState {
  readonly attachment: RealtimeSocketAttachment;
}

interface RealtimeDeliveryMessage {
  readonly result: unknown;
  readonly sequence: number | null;
  readonly subscriptionId: string;
}

export interface RealtimeDeliveryResult {
  readonly delivered: number;
  readonly deliveredSubscriptions: readonly string[];
}

export interface PendingRealtimeDelivery {
  readonly dependencies: RealtimeDependencySet;
  readonly message: RealtimeDeliveryMessage;
  readonly registration: StoredRealtimeRegistration;
  readonly resultJson: string;
  readonly versionSnapshot: RealtimeVersionSnapshot;
}

export interface RealtimeDeliveryGroup {
  readonly connectionKey: string;
  readonly connectionName: string;
  readonly deliveries: PendingRealtimeDelivery[];
}

export const DEFAULT_REALTIME_SHARD_COUNT = 1;

export const REALTIME_CONNECTION_SHARD_COUNT = 32;

export const DEFAULT_REALTIME_SHARD_GENERATION: RealtimeShardGeneration = {
  createdAt: 0,
  drainAfter: null,
  generationId: 1,
  status: "active",
  subscriptionShardCount: DEFAULT_REALTIME_SHARD_COUNT,
};

export const REALTIME_CATCH_UP_EVENT_LIMIT = 1000;

export const REALTIME_DELIVERY_BATCH_SIZE = 100;

// Bounds the serialized /deliver body so near-cap results cannot accumulate
// toward the 128 MB isolate memory limit (workers/platform/limits) before a
// count-based chunk fills up.
export const REALTIME_DELIVERY_BATCH_MAX_BYTES = 4 * 1024 * 1024;

// Bounds restore-time D1 work and connection DO memory for a single restore frame.
export const REALTIME_MAX_RESTORE_SUBSCRIPTIONS = 100;

export const REALTIME_MAX_ACTIVE_SUBSCRIPTIONS_PER_SOCKET = 100;

export const REALTIME_MAX_IDENTIFIER_LENGTH = 256;

// Inbound client message cap. Deliberately far below the platform's 32 MiB
// received-message ceiling (durable-objects/platform/limits): inbound messages
// are parsed, hashed, and their args persisted, so this is an abuse bound at
// the trust boundary, aligned with the 1 MB HTTP RPC body cap.
export const REALTIME_MAX_MESSAGE_BYTES = 1024 * 1024;

// Cap on a realtime query result's JSON. The binding constraint is the
// SQLite-backed DO storage limit of 2 MB combined key+value per record
// (durable-objects/platform/limits); the result shares its record with args,
// dependencies, and member registration keys, so it gets half the budget.
// Raising this later is backward-compatible; lowering it would break apps.
export const REALTIME_MAX_RESULT_JSON_BYTES = 1_000_000;

export const REALTIME_NOTIFY_EVENT_LOOKUP_ATTEMPTS = 3;

export const REALTIME_NOTIFY_EVENT_LOOKUP_RETRY_DELAY_MS = 50;

export const REALTIME_NOTIFY_SHARD_RETRY_DELAY_MS = 10;

export const REALTIME_REEVALUATION_CONCURRENCY = 8;

export const REALTIME_REEVALUATION_FAILURE_RETRY_MS = 10_000;

export const REALTIME_RECONCILIATION_INTERVAL_MS = 120_000;

export const REALTIME_MAX_SUBSCRIPTION_SHARDS = 32;

export const REALTIME_SCALE_UP_WINDOW_MS = 10 * 60 * 1000;

export const REALTIME_SCALE_DOWN_WINDOW_MS = 24 * 60 * 60 * 1000;

// Leases renew on delivery and on liveness-checked cleanup; keep the lease
// comfortably above the reconciliation interval so idle-but-live
// registrations are not expired-at-rest between renewals.
export const REALTIME_LEASE_MS = 300_000;

export const REALTIME_OUTBOX_CLEANUP_INTERVAL_MS = 60 * 60 * 1000;

export const REALTIME_OUTBOX_CLEANUP_LIMIT = 1000;

export const REALTIME_OUTBOX_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

export const REALTIME_AUTOSCALE_MIN_REGISTRATIONS_PER_SHARD = 250;

export const REALTIME_AUTOSCALE_TARGET_REGISTRATIONS_PER_SHARD = 1000;

export const REALTIME_AUTOSCALE_MAX_LATENCY_MS = 1000;

export const REALTIME_AUTOSCALE_MAX_OUTBOX_LAG_MS = 30_000;

export const REALTIME_AUTOSCALE_MAX_FAILED_DELIVERY_RATE = 0.05;

export const REALTIME_AUTOSCALE_LOW_LATENCY_MS = 250;

export const REALTIME_AUTOSCALE_LOW_OUTBOX_LAG_MS = 5000;

export const REALTIME_PENDING_WORK_LIMIT = 1000;

export const REALTIME_AUTOSCALING_METRIC =
  "baseflare.runtime.realtime.autoscaling";

export const REALTIME_BACKPRESSURE_METRIC =
  "baseflare.runtime.realtime.backpressure";

export const REALTIME_DELIVERY_BATCHES_METRIC =
  "baseflare.runtime.realtime.delivery_batches";

export const REALTIME_OUTBOX_LAG_METRIC =
  "baseflare.runtime.realtime.outbox_lag_ms";

export const REALTIME_OUTBOX_CLEANUPS_METRIC =
  "baseflare.runtime.realtime.outbox_cleanups";

export const REALTIME_RE_EVALUATIONS_METRIC =
  "baseflare.runtime.realtime.re_evaluations";

export const REALTIME_QUERY_EXECUTIONS_SAVED_METRIC =
  "baseflare.runtime.realtime.query_executions_saved";

export const REALTIME_ACTIVE_QUERIES_METRIC =
  "baseflare.runtime.realtime.active_queries";

export const REALTIME_RESTORE_SUBSCRIPTIONS_METRIC =
  "baseflare.runtime.realtime.restore_subscriptions";

export const REALTIME_RECONCILIATIONS_METRIC =
  "baseflare.runtime.realtime.reconciliations";

export const REALTIME_RUNTIME_LIMIT_EXCEEDED_METRIC =
  "baseflare.runtime.realtime.runtime_limit_exceeded";

export const JSON_HEADERS = { "content-type": "application/json" } as const;

export const REALTIME_CONNECTION_KEY_HEADER =
  "x-baseflare-realtime-connection-key";

export const REALTIME_RUNTIME_ID_HEADER = "x-baseflare-realtime-runtime-id";

export type RealtimeMetricSource = "catch_up" | "notify";

export type RealtimeMetricResult =
  | "accepted"
  | "active"
  | "cleaned"
  | "coalesced"
  | "delivered"
  | "empty"
  | "evaluated"
  | "failed"
  | "fanout"
  | "limited"
  | "limit_exceeded"
  | "partial"
  | "reconciled"
  | "rejected"
  | "retired"
  | "scaled_down"
  | "scaled_up"
  | "skipped"
  | "single"
  | "undelivered";
