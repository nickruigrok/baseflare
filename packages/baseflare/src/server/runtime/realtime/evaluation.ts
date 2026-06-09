import type { RuntimeDatabase } from "../types";
import {
  isZeroRealtimeVersionSnapshot,
  parseRealtimePartitionId,
} from "./routing";
import { fetchRealtimeVersionSnapshot } from "./shards";
import type {
  RealtimeAffectedTargets,
  RealtimeDependencySet,
  StoredRealtimeActiveQuery,
} from "./types";

/**
 * Version-first reconciliation: decides whether an active query must be
 * re-evaluated for the affected targets by comparing its captured
 * table/partition version snapshot against current versions. Unknown or
 * unparseable dependency state stays conservative and forces evaluation.
 */
export async function hasRelevantVersionGap(
  activeQuery: StoredRealtimeActiveQuery,
  targets: RealtimeAffectedTargets,
  database: RuntimeDatabase
): Promise<boolean> {
  if (
    targets.all ||
    !activeQuery.dependencies ||
    !activeQuery.versionSnapshot
  ) {
    return true;
  }
  if (
    activeQuery.dependencies.tables.size === 0 &&
    activeQuery.dependencies.partitions.size === 0
  ) {
    return true;
  }
  if (isZeroRealtimeVersionSnapshot(activeQuery.versionSnapshot)) {
    return true;
  }

  const relevant = getRelevantDependencies(activeQuery.dependencies, targets);
  if (relevant.forceEvaluation) {
    return true;
  }

  const { dependencies: relevantDependencies } = relevant;
  if (
    relevantDependencies.tables.size === 0 &&
    relevantDependencies.partitions.size === 0
  ) {
    return false;
  }

  const currentSnapshot = await fetchRealtimeVersionSnapshot(
    database,
    relevantDependencies
  );
  for (const tableName of relevantDependencies.tables) {
    if (
      currentSnapshot.tables.get(tableName) !==
      activeQuery.versionSnapshot.tables.get(tableName)
    ) {
      return true;
    }
  }

  for (const partitionId of relevantDependencies.partitions) {
    if (
      currentSnapshot.partitions.get(partitionId) !==
      activeQuery.versionSnapshot.partitions.get(partitionId)
    ) {
      return true;
    }
  }

  return false;
}

function getRelevantDependencies(
  registrationDependencies: RealtimeDependencySet,
  targets: RealtimeAffectedTargets
): {
  readonly dependencies: {
    readonly partitions: Set<string>;
    readonly tables: Set<string>;
  };
  readonly forceEvaluation: boolean;
} {
  const dependencies = {
    partitions: new Set<string>(),
    tables: new Set<string>(),
  };
  for (const tableName of registrationDependencies.tables) {
    if (!targets.tables.has(tableName)) {
      continue;
    }

    dependencies.tables.add(tableName);
  }

  for (const partitionId of registrationDependencies.partitions) {
    const partition = parseRealtimePartitionId(partitionId);
    if (!partition) {
      return { dependencies, forceEvaluation: true };
    }

    if (targets.broadTables.has(partition.tableName)) {
      return { dependencies, forceEvaluation: true };
    }

    if (!targets.partitions.has(partitionId)) {
      continue;
    }

    dependencies.partitions.add(partitionId);
  }

  return { dependencies, forceEvaluation: false };
}
