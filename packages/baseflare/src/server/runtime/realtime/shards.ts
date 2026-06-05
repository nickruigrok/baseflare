import {
  PARTITION_VERSION_TABLE_NAME,
  REALTIME_AUTOSCALE_STATE_TABLE_NAME,
  REALTIME_SHARD_CURSORS_TABLE_NAME,
  REALTIME_SHARD_GENERATIONS_TABLE_NAME,
  TABLE_VERSION_TABLE_NAME,
} from "../../schema/types";
import { bindStatement } from "../d1";
import type { RuntimeDatabase } from "../types";
import { parseRealtimePartitionId } from "./routing";
import { emitRealtimeMetric } from "./shared";
import type {
  RealtimeDependencySet,
  RealtimeMetricResult,
  RealtimePressureSnapshot,
  RealtimeShardGeneration,
  RealtimeShardGenerationStatus,
  RealtimeVersionSnapshot,
} from "./types";
import {
  DEFAULT_REALTIME_SHARD_GENERATION,
  REALTIME_AUTOSCALE_LOW_LATENCY_MS,
  REALTIME_AUTOSCALE_LOW_OUTBOX_LAG_MS,
  REALTIME_AUTOSCALE_MAX_FAILED_DELIVERY_RATE,
  REALTIME_AUTOSCALE_MAX_LATENCY_MS,
  REALTIME_AUTOSCALE_MAX_OUTBOX_LAG_MS,
  REALTIME_AUTOSCALE_MIN_REGISTRATIONS_PER_SHARD,
  REALTIME_AUTOSCALE_TARGET_REGISTRATIONS_PER_SHARD,
  REALTIME_AUTOSCALING_METRIC,
  REALTIME_LEASE_MS,
  REALTIME_MAX_SUBSCRIPTION_SHARDS,
  REALTIME_PENDING_WORK_LIMIT,
  REALTIME_SCALE_DOWN_WINDOW_MS,
  REALTIME_SCALE_UP_WINDOW_MS,
} from "./types";

async function fetchRealtimeShardGenerations(
  database: Pick<RuntimeDatabase, "prepare">,
  statuses: readonly RealtimeShardGenerationStatus[]
): Promise<RealtimeShardGeneration[]> {
  const placeholders = statuses.map(() => "?").join(", ");
  const result = await bindStatement(
    database,
    `SELECT generation_id, subscription_shard_count, status, created_at, drain_after
     FROM ${REALTIME_SHARD_GENERATIONS_TABLE_NAME}
     WHERE status IN (${placeholders})
     ORDER BY generation_id ASC`,
    statuses
  ).all<{
    created_at: number;
    drain_after: number | null;
    generation_id: number;
    status: RealtimeShardGenerationStatus;
    subscription_shard_count: number;
  }>();

  const generations = (result.results ?? []).map((row) => ({
    createdAt: row.created_at,
    drainAfter: row.drain_after,
    generationId: row.generation_id,
    status: row.status,
    subscriptionShardCount: row.subscription_shard_count,
  }));

  return generations.length > 0
    ? generations
    : [DEFAULT_REALTIME_SHARD_GENERATION];
}

export async function fetchActiveRealtimeShardGeneration(
  database: Pick<RuntimeDatabase, "prepare">
): Promise<RealtimeShardGeneration> {
  const generations = await fetchRealtimeShardGenerations(database, ["active"]);
  return generations.at(-1) ?? DEFAULT_REALTIME_SHARD_GENERATION;
}

export function fetchRoutableRealtimeShardGenerations(
  database: Pick<RuntimeDatabase, "prepare">
): Promise<RealtimeShardGeneration[]> {
  return fetchRealtimeShardGenerations(database, ["active", "draining"]);
}

export async function fetchOldestRealtimeShardCursor(
  database: Pick<RuntimeDatabase, "prepare">
): Promise<number | null> {
  const row = await bindStatement(
    database,
    `SELECT MIN(last_processed_outbox_sequence) AS sequence
     FROM ${REALTIME_SHARD_CURSORS_TABLE_NAME}
     WHERE last_processed_outbox_sequence IS NOT NULL
       AND generation_id IN (
         SELECT generation_id FROM ${REALTIME_SHARD_GENERATIONS_TABLE_NAME}
         WHERE status IN ('active', 'draining')
       )`,
    []
  ).first<{ sequence: number | null }>();

  return typeof row?.sequence === "number" ? row.sequence : null;
}

export async function recordRealtimeShardCursor(
  database: Pick<RuntimeDatabase, "prepare">,
  shardName: string,
  generationId: number,
  sequence: number | null
): Promise<void> {
  if (sequence == null) {
    return;
  }

  await bindStatement(
    database,
    `INSERT INTO ${REALTIME_SHARD_CURSORS_TABLE_NAME}
       (shard_name, generation_id, last_processed_outbox_sequence, updated_at)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(shard_name) DO UPDATE SET
       generation_id = excluded.generation_id,
       last_processed_outbox_sequence = MAX(
         COALESCE(${REALTIME_SHARD_CURSORS_TABLE_NAME}.last_processed_outbox_sequence, 0),
         excluded.last_processed_outbox_sequence
       ),
       updated_at = excluded.updated_at`,
    [shardName, generationId, sequence, Date.now()]
  ).run();
}

export async function fetchRealtimeVersionSnapshot(
  database: Pick<RuntimeDatabase, "prepare">,
  dependencies: RealtimeDependencySet
): Promise<RealtimeVersionSnapshot> {
  const tables = new Map<string, number>();
  const partitions = new Map<string, number>();

  if (dependencies.tables.size > 0) {
    const tableNames = [...dependencies.tables];
    const placeholders = tableNames.map(() => "?").join(", ");
    const result = await bindStatement(
      database,
      `SELECT table_name, version FROM ${TABLE_VERSION_TABLE_NAME}
       WHERE table_name IN (${placeholders})`,
      tableNames
    ).all<{ table_name: string; version: number }>();
    for (const row of result.results ?? []) {
      tables.set(row.table_name, row.version);
    }
  }

  const partitionRequests: Array<{
    readonly partitionId: string;
    readonly partitionKey: string;
    readonly partitionValue: string;
    readonly tableName: string;
  }> = [];
  for (const partitionId of dependencies.partitions) {
    const partition = parseRealtimePartitionId(partitionId);
    if (!partition) {
      continue;
    }

    partitionRequests.push({
      partitionId,
      partitionKey: partition.partitionKey,
      partitionValue: partition.partitionValue,
      tableName: partition.tableName,
    });
  }

  if (partitionRequests.length > 0) {
    const placeholders = partitionRequests.map(() => "(?, ?, ?, ?)").join(", ");
    const params = partitionRequests.flatMap((partition) => [
      partition.partitionId,
      partition.tableName,
      partition.partitionKey,
      partition.partitionValue,
    ]);
    const result = await bindStatement(
      database,
      `WITH requested(partition_id, table_name, partition_key, partition_value) AS (
         VALUES ${placeholders}
       )
       SELECT requested.partition_id, COALESCE(${PARTITION_VERSION_TABLE_NAME}.version, 0) AS version
       FROM requested
       LEFT JOIN ${PARTITION_VERSION_TABLE_NAME}
         ON ${PARTITION_VERSION_TABLE_NAME}.table_name = requested.table_name
        AND ${PARTITION_VERSION_TABLE_NAME}.partition_key = requested.partition_key
        AND ${PARTITION_VERSION_TABLE_NAME}.partition_value = requested.partition_value`,
      params
    ).all<{ partition_id: string; version: number }>();
    for (const row of result.results ?? []) {
      partitions.set(row.partition_id, row.version);
    }
  }

  return { partitions, tables };
}

export async function evaluateRealtimeAutoscaling(
  database: Pick<RuntimeDatabase, "batch" | "prepare">,
  input: {
    readonly pressure: RealtimePressureSnapshot;
    readonly now?: number;
  }
): Promise<RealtimeMetricResult | null> {
  const now = input.now ?? Date.now();
  await retireDrainedRealtimeShardGenerations(database, now);
  const activeGeneration = await fetchActiveRealtimeShardGeneration(database);
  const targetRegistrations =
    activeGeneration.subscriptionShardCount *
    REALTIME_AUTOSCALE_TARGET_REGISTRATIONS_PER_SHARD;
  const lowRegistrations =
    activeGeneration.subscriptionShardCount *
    REALTIME_AUTOSCALE_MIN_REGISTRATIONS_PER_SHARD;
  const state = await bindStatement(
    database,
    `SELECT scale_up_started_at, scale_down_started_at
     FROM ${REALTIME_AUTOSCALE_STATE_TABLE_NAME}
     WHERE id = 1`,
    []
  ).first<{
    scale_down_started_at: number | null;
    scale_up_started_at: number | null;
  }>();
  const highPressure = hasHighRealtimePressure(
    input.pressure,
    targetRegistrations
  );
  const lowPressure = hasLowRealtimePressure(input.pressure, lowRegistrations);

  if (
    highPressure &&
    activeGeneration.subscriptionShardCount < REALTIME_MAX_SUBSCRIPTION_SHARDS
  ) {
    const startedAt = state?.scale_up_started_at ?? now;
    if (now - startedAt >= REALTIME_SCALE_UP_WINDOW_MS) {
      const nextShardCount = Math.min(
        activeGeneration.subscriptionShardCount * 2,
        REALTIME_MAX_SUBSCRIPTION_SHARDS
      );
      await createRealtimeShardGeneration(database, activeGeneration, {
        now,
        result: "scaled_up",
        shardCount: nextShardCount,
      });
      return "scaled_up";
    }

    await updateRealtimeAutoscaleState(database, {
      now,
      scaleDownStartedAt: null,
      scaleUpStartedAt: startedAt,
    });
    return null;
  }

  if (lowPressure && activeGeneration.subscriptionShardCount > 1) {
    const startedAt = state?.scale_down_started_at ?? now;
    if (now - startedAt >= REALTIME_SCALE_DOWN_WINDOW_MS) {
      const nextShardCount = Math.max(
        Math.floor(activeGeneration.subscriptionShardCount / 2),
        1
      );
      await createRealtimeShardGeneration(database, activeGeneration, {
        now,
        result: "scaled_down",
        shardCount: nextShardCount,
      });
      return "scaled_down";
    }

    await updateRealtimeAutoscaleState(database, {
      now,
      scaleDownStartedAt: startedAt,
      scaleUpStartedAt: null,
    });
    return null;
  }

  await updateRealtimeAutoscaleState(database, {
    now,
    scaleDownStartedAt: null,
    scaleUpStartedAt: null,
  });
  return null;
}

function hasHighRealtimePressure(
  pressure: RealtimePressureSnapshot,
  targetRegistrations: number
): boolean {
  return (
    pressure.activeRegistrationCount >= targetRegistrations ||
    (pressure.pendingWorkCount ?? 0) >= REALTIME_PENDING_WORK_LIMIT ||
    (pressure.reEvaluationLatencyMs ?? 0) >=
      REALTIME_AUTOSCALE_MAX_LATENCY_MS ||
    (pressure.deliveryBatchLatencyMs ?? 0) >=
      REALTIME_AUTOSCALE_MAX_LATENCY_MS ||
    (pressure.outboxLagMs ?? 0) >= REALTIME_AUTOSCALE_MAX_OUTBOX_LAG_MS ||
    (pressure.failedDeliveryRate ?? 0) >=
      REALTIME_AUTOSCALE_MAX_FAILED_DELIVERY_RATE
  );
}

function hasLowRealtimePressure(
  pressure: RealtimePressureSnapshot,
  lowRegistrations: number
): boolean {
  return (
    pressure.activeRegistrationCount <= lowRegistrations &&
    (pressure.pendingWorkCount ?? 0) === 0 &&
    (pressure.reEvaluationLatencyMs ?? 0) < REALTIME_AUTOSCALE_LOW_LATENCY_MS &&
    (pressure.deliveryBatchLatencyMs ?? 0) <
      REALTIME_AUTOSCALE_LOW_LATENCY_MS &&
    (pressure.outboxLagMs ?? 0) < REALTIME_AUTOSCALE_LOW_OUTBOX_LAG_MS &&
    (pressure.failedDeliveryRate ?? 0) === 0
  );
}

async function retireDrainedRealtimeShardGenerations(
  database: Pick<RuntimeDatabase, "prepare">,
  now: number
): Promise<void> {
  const result = await bindStatement(
    database,
    `UPDATE ${REALTIME_SHARD_GENERATIONS_TABLE_NAME}
     SET status = 'retired'
     WHERE status = 'draining' AND drain_after IS NOT NULL AND drain_after <= ?`,
    [now]
  ).run();
  const retiredCount = result.meta?.changes ?? 0;
  if (retiredCount > 0) {
    emitRealtimeMetric(REALTIME_AUTOSCALING_METRIC, retiredCount, {
      result: "retired",
    });
  }
}

export function evaluateRealtimeAutoscalingForTest(
  database: Pick<RuntimeDatabase, "batch" | "prepare">,
  input: {
    readonly activeRegistrationCount: number;
    readonly deliveryBatchLatencyMs?: number;
    readonly failedDeliveryRate?: number;
    readonly now?: number;
    readonly outboxLagMs?: number;
    readonly pendingWorkCount?: number;
    readonly reEvaluationLatencyMs?: number;
  }
): Promise<RealtimeMetricResult | null> {
  return evaluateRealtimeAutoscaling(database, {
    now: input.now,
    pressure: {
      activeRegistrationCount: input.activeRegistrationCount,
      deliveryBatchLatencyMs: input.deliveryBatchLatencyMs,
      failedDeliveryRate: input.failedDeliveryRate,
      outboxLagMs: input.outboxLagMs,
      pendingWorkCount: input.pendingWorkCount,
      reEvaluationLatencyMs: input.reEvaluationLatencyMs,
    },
  });
}

async function createRealtimeShardGeneration(
  database: Pick<RuntimeDatabase, "batch" | "prepare">,
  activeGeneration: RealtimeShardGeneration,
  input: {
    readonly now: number;
    readonly result: "scaled_down" | "scaled_up";
    readonly shardCount: number;
  }
): Promise<void> {
  const nextGenerationId = activeGeneration.generationId + 1;
  const statements = [
    bindStatement(
      database,
      `UPDATE ${REALTIME_SHARD_GENERATIONS_TABLE_NAME}
       SET status = 'draining', drain_after = ?
       WHERE generation_id = ? AND status = 'active'`,
      [input.now + REALTIME_LEASE_MS, activeGeneration.generationId]
    ),
    bindStatement(
      database,
      `INSERT INTO ${REALTIME_SHARD_GENERATIONS_TABLE_NAME}
         (generation_id, subscription_shard_count, status, created_at, drain_after)
       VALUES (?, ?, 'active', ?, NULL)`,
      [nextGenerationId, input.shardCount, input.now]
    ),
    bindStatement(
      database,
      `UPDATE ${REALTIME_AUTOSCALE_STATE_TABLE_NAME}
       SET scale_up_started_at = NULL, scale_down_started_at = NULL, updated_at = ?
       WHERE id = 1`,
      [input.now]
    ),
  ];

  await database.batch(statements);
  emitRealtimeMetric(REALTIME_AUTOSCALING_METRIC, input.shardCount, {
    result: input.result,
  });
}

async function updateRealtimeAutoscaleState(
  database: Pick<RuntimeDatabase, "prepare">,
  input: {
    readonly now: number;
    readonly scaleDownStartedAt: number | null;
    readonly scaleUpStartedAt: number | null;
  }
): Promise<void> {
  await bindStatement(
    database,
    `INSERT INTO ${REALTIME_AUTOSCALE_STATE_TABLE_NAME}
       (id, scale_up_started_at, scale_down_started_at, updated_at)
     VALUES (1, ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET
       scale_up_started_at = excluded.scale_up_started_at,
       scale_down_started_at = excluded.scale_down_started_at,
       updated_at = excluded.updated_at`,
    [input.scaleUpStartedAt, input.scaleDownStartedAt, input.now]
  ).run();
}
