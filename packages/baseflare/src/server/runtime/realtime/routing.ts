import type { RuntimeReadObserver } from "../d1";
import { InternalRuntimeError } from "../errors";
import { partitionTargetId } from "../partitioning";
import type {
  RealtimeAffectedTargets,
  RealtimeDependencySet,
  RealtimeOutboxEvent,
  RealtimePartitionTarget,
  RealtimeSequencedOutboxEvent,
  RealtimeShardGeneration,
  RealtimeSubscriptionRouteTarget,
  RealtimeVersionSnapshot,
} from "./types";
import {
  DEFAULT_REALTIME_SHARD_COUNT,
  DEFAULT_REALTIME_SHARD_GENERATION,
  REALTIME_CONNECTION_SHARD_COUNT,
} from "./types";

const UINT32_MODULUS = 4_294_967_296;

export function createRegistrationKey(
  connectionKey: string,
  subscriptionId: string
): string {
  return JSON.stringify([connectionKey, subscriptionId]);
}

export function createRealtimeDependencySet(): {
  readonly dependencies: RealtimeDependencySet;
  readonly readObserver: RuntimeReadObserver;
} {
  const tables = new Set<string>();
  const partitions = new Set<string>();
  return {
    dependencies: { partitions, tables },
    readObserver: {
      onPartitionRead(partition) {
        partitions.add(partitionTargetId(partition));
      },
      onTableRead(tableName) {
        tables.add(tableName);
      },
    },
  };
}

export function createRealtimeAffectedTargets(
  events: readonly RealtimeSequencedOutboxEvent[]
): RealtimeAffectedTargets {
  const broadTables = new Set<string>();
  const partitions = new Set<string>();
  const tables = new Set<string>();
  let sequence: number | null = null;

  for (const event of events) {
    sequence = Math.max(sequence ?? 0, event.sequence);
    const partitionTables = new Set<string>();
    for (const partition of event.partitions) {
      partitions.add(partitionTargetId(partition));
      partitionTables.add(partition.tableName);
    }

    for (const tableName of event.tables) {
      tables.add(tableName);
      if (!partitionTables.has(tableName)) {
        broadTables.add(tableName);
      }
    }
  }

  return { all: false, broadTables, partitions, sequence, tables };
}

export function createFullRealtimeAffectedTargets(
  sequence: number | null
): RealtimeAffectedTargets {
  return {
    all: true,
    broadTables: new Set<string>(),
    partitions: new Set<string>(),
    sequence,
    tables: new Set<string>(),
  };
}

export function getPartitionDependencyTable(
  partitionId: string
): string | undefined {
  try {
    const parsed = JSON.parse(partitionId) as unknown;
    return Array.isArray(parsed) && typeof parsed[0] === "string"
      ? parsed[0]
      : undefined;
  } catch {
    return undefined;
  }
}

export function parseRealtimePartitionId(
  partitionId: string
): RealtimePartitionTarget | null {
  try {
    const parsed = JSON.parse(partitionId) as unknown;
    if (
      !Array.isArray(parsed) ||
      typeof parsed[0] !== "string" ||
      typeof parsed[1] !== "string" ||
      typeof parsed[2] !== "string"
    ) {
      return null;
    }

    return {
      partitionKey: parsed[1],
      partitionValue: parsed[2],
      tableName: parsed[0],
    };
  } catch {
    return null;
  }
}

export function serializeRealtimeDependencySet(
  dependencies: RealtimeDependencySet
): {
  readonly partitions: string[];
  readonly tables: string[];
} {
  return {
    partitions: [...dependencies.partitions],
    tables: [...dependencies.tables],
  };
}

export function parseRealtimeDependencySetValue(
  value: unknown
): RealtimeDependencySet | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }

  const input = value as Record<string, unknown>;
  const partitions = Array.isArray(input.partitions)
    ? input.partitions.filter((partition): partition is string => {
        return typeof partition === "string";
      })
    : [];
  const tables = Array.isArray(input.tables)
    ? input.tables.filter((tableName): tableName is string => {
        return typeof tableName === "string";
      })
    : [];

  return {
    partitions: new Set(partitions),
    tables: new Set(tables),
  };
}

export function serializeRealtimeVersionSnapshot(
  snapshot: RealtimeVersionSnapshot
): {
  readonly partitions: Array<readonly [string, number]>;
  readonly tables: Array<readonly [string, number]>;
} {
  return {
    partitions: [...snapshot.partitions],
    tables: [...snapshot.tables],
  };
}

export function parseRealtimeVersionSnapshotValue(
  value: unknown
): RealtimeVersionSnapshot | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }

  const input = value as Record<string, unknown>;
  const partitions = new Map<string, number>();
  const tables = new Map<string, number>();
  if (Array.isArray(input.partitions)) {
    for (const entry of input.partitions) {
      if (
        Array.isArray(entry) &&
        typeof entry[0] === "string" &&
        typeof entry[1] === "number"
      ) {
        partitions.set(entry[0], entry[1]);
      }
    }
  }
  if (Array.isArray(input.tables)) {
    for (const entry of input.tables) {
      if (
        Array.isArray(entry) &&
        typeof entry[0] === "string" &&
        typeof entry[1] === "number"
      ) {
        tables.set(entry[0], entry[1]);
      }
    }
  }

  return { partitions, tables };
}

export function getRealtimeRegistrationHomeRouteTarget(
  dependencies: RealtimeDependencySet | undefined
): RealtimeSubscriptionRouteTarget {
  if (!dependencies) {
    return createRealtimeGlobalSubscriptionRouteTarget();
  }

  if (dependencies.tables.size === 0 && dependencies.partitions.size === 1) {
    const partitionId = dependencies.partitions.values().next().value;
    const partition =
      typeof partitionId === "string"
        ? parseRealtimePartitionId(partitionId)
        : null;
    return partition
      ? createRealtimePartitionSubscriptionRouteTarget(partition)
      : createRealtimeGlobalSubscriptionRouteTarget();
  }

  if (dependencies.tables.size === 1 && dependencies.partitions.size === 0) {
    const tableName = dependencies.tables.values().next().value;
    return typeof tableName === "string"
      ? createRealtimeTableSubscriptionRouteTarget(tableName)
      : createRealtimeGlobalSubscriptionRouteTarget();
  }

  return createRealtimeGlobalSubscriptionRouteTarget();
}

export function isZeroRealtimeVersionSnapshot(
  snapshot: RealtimeVersionSnapshot
): boolean {
  if (snapshot.tables.size === 0 && snapshot.partitions.size === 0) {
    return false;
  }

  for (const version of snapshot.tables.values()) {
    if (version > 0) {
      return false;
    }
  }

  for (const version of snapshot.partitions.values()) {
    if (version > 0) {
      return false;
    }
  }

  return true;
}

function getRealtimeShardName(
  prefix: string,
  key: string,
  shardCount = DEFAULT_REALTIME_SHARD_COUNT
): string {
  if (!Number.isInteger(shardCount) || shardCount <= 0) {
    throw new InternalRuntimeError(
      "Realtime shard count must be a positive integer"
    );
  }

  let hash = 0;
  for (const char of key) {
    const nextHash = Math.imul(hash, 31) + char.charCodeAt(0);
    hash = ((nextHash % UINT32_MODULUS) + UINT32_MODULUS) % UINT32_MODULUS;
  }

  return `${prefix}:${hash % shardCount}`;
}

export function getRealtimeShardGenerationIdFromName(
  shardName: string
): number {
  const match = /^subscription:g(\d+):\d+$/.exec(shardName);
  if (!match) {
    return DEFAULT_REALTIME_SHARD_GENERATION.generationId;
  }

  return Number(match[1]);
}

export function getRealtimeConnectionShardName(
  key: string,
  shardCount = REALTIME_CONNECTION_SHARD_COUNT
): string {
  return getRealtimeShardName("connection", key, shardCount);
}

export function createRealtimeGlobalSubscriptionRouteTarget(): RealtimeSubscriptionRouteTarget {
  return { type: "global" };
}

export function createRealtimeTableSubscriptionRouteTarget(
  tableName: string
): RealtimeSubscriptionRouteTarget {
  return { tableName, type: "table" };
}

export function createRealtimePartitionSubscriptionRouteTarget(
  partition: RealtimePartitionTarget
): RealtimeSubscriptionRouteTarget {
  return { partition, type: "partition" };
}

function getRealtimeSubscriptionRouteTargetKey(
  route: RealtimeSubscriptionRouteTarget
): string {
  if (route.type === "global") {
    return "global";
  }

  if (route.type === "table") {
    return JSON.stringify(["table", route.tableName]);
  }

  return JSON.stringify(["partition", partitionTargetId(route.partition)]);
}

export function getRealtimeSubscriptionShardName(
  route: RealtimeSubscriptionRouteTarget = createRealtimeGlobalSubscriptionRouteTarget(),
  generationOrShardCount:
    | number
    | Pick<
        RealtimeShardGeneration,
        "generationId" | "subscriptionShardCount"
      > = DEFAULT_REALTIME_SHARD_GENERATION
): string {
  const generation =
    typeof generationOrShardCount === "number"
      ? {
          generationId: DEFAULT_REALTIME_SHARD_GENERATION.generationId,
          subscriptionShardCount: generationOrShardCount,
        }
      : generationOrShardCount;
  const routeShardName = getRealtimeShardName(
    `subscription:g${generation.generationId}`,
    getRealtimeSubscriptionRouteTargetKey(route),
    generation.subscriptionShardCount
  );
  return routeShardName;
}

export function getRealtimeSubscriptionShardNames(
  generation: Pick<
    RealtimeShardGeneration,
    "generationId" | "subscriptionShardCount"
  >
): string[] {
  return Array.from(
    { length: generation.subscriptionShardCount },
    (_value, index) => `subscription:g${generation.generationId}:${index}`
  );
}

export function getRealtimeAffectedSubscriptionRouteTargets(
  event: Pick<RealtimeOutboxEvent, "partitions" | "tables">
): RealtimeSubscriptionRouteTarget[] {
  const routes = new Map<string, RealtimeSubscriptionRouteTarget>();
  const globalRoute = createRealtimeGlobalSubscriptionRouteTarget();
  routes.set(getRealtimeSubscriptionRouteTargetKey(globalRoute), globalRoute);

  for (const partition of event.partitions) {
    const route = createRealtimePartitionSubscriptionRouteTarget(partition);
    routes.set(getRealtimeSubscriptionRouteTargetKey(route), route);
  }

  for (const tableName of event.tables) {
    const route = createRealtimeTableSubscriptionRouteTarget(tableName);
    routes.set(getRealtimeSubscriptionRouteTargetKey(route), route);
  }

  return [...routes.values()];
}
