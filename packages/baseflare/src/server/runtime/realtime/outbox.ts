import { generateId } from "baseflare/values";
import { REALTIME_OUTBOX_TABLE_NAME } from "../../schema/types";
import { bindStatement } from "../d1";
import { InternalRuntimeError } from "../errors";
import { logRuntimeEvent } from "../logging";
import type {
  BaseflareExecutionContext,
  BaseflareRuntimeEnv,
  DurableObjectNamespace,
  DurableObjectStub,
  RuntimeDatabase,
} from "../types";
import {
  getRealtimeAffectedSubscriptionRouteTargets,
  getRealtimeSubscriptionShardName,
} from "./routing";
import { fetchRoutableRealtimeShardGenerations } from "./shards";
import type {
  RealtimeMutationNotifier,
  RealtimeOutboxEvent,
  RealtimeOutboxOperation,
  RealtimePartitionTarget,
  RealtimeSequencedOutboxEvent,
  RealtimeShardGeneration,
  RealtimeSubscriptionRouteTarget,
} from "./types";
import {
  JSON_HEADERS,
  REALTIME_CATCH_UP_EVENT_LIMIT,
  REALTIME_NOTIFY_SHARD_RETRY_DELAY_MS,
  REALTIME_OUTBOX_CLEANUP_LIMIT,
} from "./types";

interface RealtimeOutboxRow {
  readonly created_at: number;
  readonly event_id: string;
  readonly partitions: string;
  readonly sequence: number;
  readonly tables: string;
}

type RealtimeOutboxDatabase = Pick<RuntimeDatabase, "prepare"> & {
  readonly withSession?: (
    constraint?: string
  ) => Pick<RuntimeDatabase, "prepare">;
};

class RealtimeNotifyFailure extends InternalRuntimeError {
  readonly retryable: boolean;

  constructor(message: string, retryable: boolean) {
    super(message);
    this.retryable = retryable;
  }
}

export interface RealtimeOutboxFetchResult {
  readonly events: RealtimeSequencedOutboxEvent[];
  readonly hasMalformedEvents: boolean;
  readonly latestReadSequence: number | null;
}

export function createRealtimeOutboxResponseEvents(
  events: readonly RealtimeSequencedOutboxEvent[]
): Array<RealtimeOutboxEvent & { readonly sequence: number }> {
  return events.map((event) => ({
    eventId: event.eventId,
    partitions: event.partitions,
    sequence: event.sequence,
    tables: event.tables,
  }));
}

export function createRealtimeOutboxOperation(
  event: RealtimeOutboxEvent,
  expectedPreviousChanges: number
): RealtimeOutboxOperation {
  return {
    event,
    // The SQL guard can skip this insert if the mutation chain failed; the
    // expected change count below makes that fail closed during commit.
    expectedChanges: 1,
    params: [
      event.eventId,
      JSON.stringify(event.tables),
      JSON.stringify(event.partitions),
      expectedPreviousChanges,
    ],
    sql: `INSERT INTO ${REALTIME_OUTBOX_TABLE_NAME} (event_id, created_at, tables, partitions)
          SELECT ?, CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER), ?, ?
          WHERE changes() = ?`,
    type: "insert-realtime-outbox",
  };
}

export function createRealtimeOutboxEvent(
  tableNames: readonly string[],
  partitions: readonly RealtimePartitionTarget[]
): RealtimeOutboxEvent {
  return {
    eventId: generateId(),
    partitions: [...partitions].sort((left, right) =>
      `${left.tableName}/${left.partitionKey}/${left.partitionValue}`.localeCompare(
        `${right.tableName}/${right.partitionKey}/${right.partitionValue}`
      )
    ),
    tables: [...tableNames].sort(),
  };
}

export function createRealtimeMutationNotifier(
  env: BaseflareRuntimeEnv,
  ctx: BaseflareExecutionContext
): RealtimeMutationNotifier | undefined {
  if (!env.REALTIME_SUBSCRIPTIONS) {
    return undefined;
  }
  const subscriptionNamespace = env.REALTIME_SUBSCRIPTIONS;

  return {
    enabled: true,
    notify(events, options) {
      for (const event of events) {
        ctx.waitUntil(
          notifyRealtimeSubscriptionShards(
            env.APP_DB,
            subscriptionNamespace,
            event,
            options?.outboxBookmark ?? null
          ).catch((error: unknown) => {
            logRuntimeEvent("error", "runtime.realtime_notify_failed", {
              errorName: error instanceof Error ? error.name : typeof error,
              eventId: event.eventId,
            });
          })
        );
      }
    },
  };
}

async function notifyRealtimeSubscriptionShards(
  database: RealtimeOutboxDatabase,
  namespace: DurableObjectNamespace,
  event: RealtimeOutboxEvent,
  outboxBookmark: string | null
): Promise<void> {
  const generations = await fetchRoutableRealtimeShardGenerations(database);
  const stubs = getRealtimeSubscriptionStubs(
    namespace,
    getRealtimeAffectedSubscriptionRouteTargets(event),
    generations
  );
  const results = await Promise.allSettled(
    stubs.map(async ({ generation, shardName, stub }) => {
      try {
        await notifyRealtimeSubscriptionShard(
          stub,
          event.eventId,
          shardName,
          outboxBookmark
        );
      } catch (error) {
        if (!isRetryableRealtimeNotifyError(error)) {
          logRuntimeEvent("error", "runtime.realtime_notify_failed", {
            errorName: error instanceof Error ? error.name : typeof error,
            eventId: event.eventId,
            generationId: generation.generationId,
            shardName,
          });
          throw error;
        }

        try {
          await catchUpRealtimeSubscriptionShard(
            database,
            stub,
            event.eventId,
            shardName,
            outboxBookmark
          );
        } catch (catchUpError) {
          logRuntimeEvent("error", "runtime.realtime_notify_failed", {
            errorName:
              catchUpError instanceof Error
                ? catchUpError.name
                : typeof catchUpError,
            eventId: event.eventId,
            generationId: generation.generationId,
            shardName,
          });
          throw catchUpError;
        }
      }
    })
  );
  const failures = results.filter((result) => result.status === "rejected");
  if (failures.length === 1 && failures[0]) {
    throw failures[0].reason;
  }

  if (failures.length > 0) {
    throw new InternalRuntimeError(
      `Realtime notify failed for ${failures.length} shards`
    );
  }
}

async function catchUpRealtimeSubscriptionShard(
  database: RealtimeOutboxDatabase,
  stub: DurableObjectStub,
  eventId: string,
  shardName: string,
  outboxBookmark: string | null
): Promise<void> {
  const eventDatabase =
    outboxBookmark && database.withSession
      ? database.withSession(outboxBookmark)
      : database;
  const event = await fetchRealtimeOutboxEventById(eventDatabase, eventId);
  if (!event) {
    throw new InternalRuntimeError(
      `Realtime notify recovery could not find outbox event ${eventId}`
    );
  }

  const response = await stub.fetch("https://baseflare.internal/catch-up", {
    body: JSON.stringify({
      afterSequence: event.sequence - 1,
      outboxBookmark,
      shardName,
    }),
    headers: JSON_HEADERS,
    method: "POST",
  });
  if (!response.ok) {
    throw new InternalRuntimeError(
      `Realtime notify recovery failed for ${shardName} with status ${response.status}`
    );
  }
}

async function notifyRealtimeSubscriptionShard(
  stub: DurableObjectStub,
  eventId: string,
  shardName: string,
  outboxBookmark: string | null
): Promise<void> {
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    try {
      const response = await stub.fetch("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId, outboxBookmark, shardName }),
        headers: JSON_HEADERS,
        method: "POST",
      });
      if (response.ok) {
        return;
      }

      throw new RealtimeNotifyFailure(
        `Realtime notify failed for ${shardName} with status ${response.status}`,
        isRetryableRealtimeNotifyStatus(response.status)
      );
    } catch (error) {
      if (attempt >= 2 || !isRetryableRealtimeNotifyError(error)) {
        throw error;
      }
      await waitForRealtimeNotifyShardRetry();
    }
  }
}

function isRetryableRealtimeNotifyError(error: unknown): boolean {
  if (error instanceof RealtimeNotifyFailure) {
    return error.retryable;
  }

  return true;
}

function isRetryableRealtimeNotifyStatus(status: number): boolean {
  return (
    status === 408 ||
    status === 409 ||
    status === 425 ||
    status === 429 ||
    status >= 500
  );
}

function waitForRealtimeNotifyShardRetry(): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, REALTIME_NOTIFY_SHARD_RETRY_DELAY_MS);
  });
}

function getRealtimeSubscriptionStubs(
  namespace: DurableObjectNamespace,
  routes: readonly RealtimeSubscriptionRouteTarget[],
  generations: readonly RealtimeShardGeneration[]
): Array<{
  readonly generation: RealtimeShardGeneration;
  readonly shardName: string;
  readonly stub: DurableObjectStub;
}> {
  const stubs = new Map<
    string,
    {
      readonly generation: RealtimeShardGeneration;
      readonly shardName: string;
      readonly stub: DurableObjectStub;
    }
  >();
  for (const generation of generations) {
    for (const route of routes) {
      const shardName = getRealtimeSubscriptionShardName(route, generation);
      stubs.set(shardName, {
        generation,
        shardName,
        stub: namespace.get(namespace.idFromName(shardName)),
      });
    }
  }

  return [...stubs.values()];
}

export async function fetchRealtimeOutboxEvents(
  database: Pick<RuntimeDatabase, "prepare">,
  afterSequence: number | null,
  limit: number
): Promise<RealtimeOutboxFetchResult> {
  const boundedLimit = Math.min(
    Math.max(limit, 1),
    REALTIME_CATCH_UP_EVENT_LIMIT
  );
  const sql =
    afterSequence === null
      ? `SELECT sequence, event_id, created_at, tables, partitions FROM ${REALTIME_OUTBOX_TABLE_NAME} ORDER BY sequence ASC LIMIT ?`
      : `SELECT sequence, event_id, created_at, tables, partitions FROM ${REALTIME_OUTBOX_TABLE_NAME} WHERE sequence > ? ORDER BY sequence ASC LIMIT ?`;
  const params =
    afterSequence === null ? [boundedLimit] : [afterSequence, boundedLimit];
  const result = await bindStatement(
    database,
    sql,
    params
  ).all<RealtimeOutboxRow>();
  const events: RealtimeSequencedOutboxEvent[] = [];
  let hasMalformedEvents = false;
  let latestReadSequence: number | null = null;
  for (const row of result.results ?? []) {
    latestReadSequence = Math.max(latestReadSequence ?? 0, row.sequence);
    const event = parseRealtimeOutboxRow(row);
    if (!event) {
      hasMalformedEvents = true;
      continue;
    }

    events.push(event);
  }

  return { events, hasMalformedEvents, latestReadSequence };
}

export async function fetchRealtimeOutboxEventById(
  database: Pick<RuntimeDatabase, "prepare">,
  eventId: string
): Promise<{
  readonly createdAt: number;
  readonly eventId: string;
  readonly partitions: readonly RealtimePartitionTarget[];
  readonly sequence: number;
  readonly tables: readonly string[];
} | null> {
  const row = await bindStatement(
    database,
    `SELECT sequence, event_id, created_at, tables, partitions FROM ${REALTIME_OUTBOX_TABLE_NAME} WHERE event_id = ?`,
    [eventId]
  ).first<RealtimeOutboxRow>();
  if (!row) {
    return null;
  }

  return parseRealtimeOutboxRow(row);
}

export async function fetchRealtimeOutboxEventSequenceById(
  database: Pick<RuntimeDatabase, "prepare">,
  eventId: string
): Promise<number | null> {
  const row = await bindStatement(
    database,
    `SELECT sequence FROM ${REALTIME_OUTBOX_TABLE_NAME} WHERE event_id = ?`,
    [eventId]
  ).first<{ sequence: number }>();
  return typeof row?.sequence === "number" ? row.sequence : null;
}

function parseRealtimeOutboxRow(
  row: RealtimeOutboxRow
): RealtimeSequencedOutboxEvent | null {
  try {
    const tables = JSON.parse(row.tables) as unknown;
    const partitions = JSON.parse(row.partitions) as unknown;
    const hasValidTables =
      Array.isArray(tables) &&
      tables.every((tableName): tableName is string => {
        return typeof tableName === "string";
      });
    const hasValidPartitions =
      Array.isArray(partitions) && partitions.every(isRealtimePartitionTarget);
    if (!(hasValidTables && hasValidPartitions)) {
      throw new Error("Malformed realtime outbox event");
    }

    return {
      createdAt: row.created_at,
      eventId: row.event_id,
      partitions,
      sequence: row.sequence,
      tables,
    };
  } catch (error) {
    logRuntimeEvent("error", "runtime.realtime_outbox_event_parse_failed", {
      errorName: error instanceof Error ? error.name : typeof error,
      eventId: row.event_id,
      sequence: row.sequence,
    });
    return null;
  }
}

function isRealtimePartitionTarget(
  value: unknown
): value is RealtimePartitionTarget {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const partition = value as Record<string, unknown>;
  return (
    typeof partition.tableName === "string" &&
    typeof partition.partitionKey === "string" &&
    typeof partition.partitionValue === "string"
  );
}

export async function fetchRealtimeOutboxHistoryGap(
  database: Pick<RuntimeDatabase, "prepare">,
  afterSequence: number | null
): Promise<{
  readonly hasGap: boolean;
  readonly latestSequence: number | null;
}> {
  if (afterSequence === null) {
    return { hasGap: false, latestSequence: null };
  }

  const row = await bindStatement(
    database,
    `SELECT MIN(sequence) AS oldest_sequence, MAX(sequence) AS latest_sequence FROM ${REALTIME_OUTBOX_TABLE_NAME}`,
    []
  ).first<{
    oldest_sequence: number | null;
    latest_sequence: number | null;
  }>();

  const oldestSequence =
    typeof row?.oldest_sequence === "number" ? row.oldest_sequence : null;
  const latestSequence =
    typeof row?.latest_sequence === "number" ? row.latest_sequence : null;

  return {
    hasGap: oldestSequence !== null && afterSequence + 1 < oldestSequence,
    latestSequence,
  };
}

export async function hasRealtimeOutboxEvents(
  database: Pick<RuntimeDatabase, "prepare">
): Promise<boolean> {
  const row = await bindStatement(
    database,
    `SELECT 1 AS has_events FROM ${REALTIME_OUTBOX_TABLE_NAME} LIMIT 1`,
    []
  ).first<{ has_events: number }>();
  return row?.has_events === 1;
}

export async function deleteRealtimeOutboxEventsBefore(
  database: Pick<RuntimeDatabase, "prepare">,
  createdBefore: number,
  limit: number,
  protectedSequence: number | null = null
): Promise<number> {
  const boundedLimit = Math.min(
    Math.max(limit, 1),
    REALTIME_OUTBOX_CLEANUP_LIMIT
  );
  const sequenceGuard = protectedSequence == null ? "" : "AND sequence < ?";
  const params =
    protectedSequence == null
      ? [createdBefore, boundedLimit]
      : [createdBefore, protectedSequence, boundedLimit];
  const result = await bindStatement(
    database,
    `DELETE FROM ${REALTIME_OUTBOX_TABLE_NAME}
     WHERE sequence IN (
       SELECT sequence FROM ${REALTIME_OUTBOX_TABLE_NAME}
       WHERE created_at < ?
       ${sequenceGuard}
       ORDER BY created_at ASC
       LIMIT ?
     )`,
    params
  ).run();
  return result.meta?.changes ?? 0;
}
