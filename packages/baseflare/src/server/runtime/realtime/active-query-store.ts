import { createRealtimeActiveQueryKey } from "./evaluation-key";
import {
  createRegistrationKey,
  getPartitionDependencyTable,
  parseRealtimeDependencySetValue,
  parseRealtimeVersionSnapshotValue,
  serializeRealtimeDependencySet,
  serializeRealtimeVersionSnapshot,
} from "./routing";
import type {
  RealtimeAffectedTargets,
  RealtimeDurableObjectState,
  StoredRealtimeActiveQuery,
  StoredRealtimeRegistration,
} from "./types";
import { REALTIME_REEVALUATION_FAILURE_RETRY_MS } from "./types";

const REALTIME_ACTIVE_QUERY_STORAGE_PREFIX = "realtime:active-query:";

type StoredRealtimeActiveQueryValue = Omit<
  StoredRealtimeActiveQuery,
  "dependencies" | "memberRegistrationKeys" | "versionSnapshot"
> & {
  readonly dependencies?: {
    readonly partitions: readonly string[];
    readonly tables: readonly string[];
  };
  readonly memberRegistrationKeys: readonly string[];
  readonly versionSnapshot?: {
    readonly partitions: ReadonlyArray<readonly [string, number]>;
    readonly tables: ReadonlyArray<readonly [string, number]>;
  };
};

export interface RealtimeActiveQueryStoreSnapshot {
  readonly activeQueries: Map<string, StoredRealtimeActiveQuery>;
  readonly activeQueryKeysByPartition: Map<string, Set<string>>;
  readonly activeQueryKeysByTable: Map<string, Set<string>>;
  readonly activeQueryKeysWithoutDependencies: Set<string>;
}

export class RealtimeActiveQueryStore {
  private readonly activeQueries = new Map<string, StoredRealtimeActiveQuery>();
  private readonly activeQueryKeysByPartition = new Map<string, Set<string>>();
  private readonly activeQueryKeysByTable = new Map<string, Set<string>>();
  private readonly activeQueryKeysWithoutDependencies = new Set<string>();
  private readonly state: RealtimeDurableObjectState;
  private loaded = false;

  constructor(state: RealtimeDurableObjectState) {
    this.state = state;
  }

  async loadOnce(): Promise<void> {
    if (this.loaded) {
      return;
    }

    const storage = this.state.storage;
    if (!storage?.list) {
      this.loaded = true;
      return;
    }

    const storedActiveQueries =
      await storage.list<StoredRealtimeActiveQueryValue>({
        prefix: REALTIME_ACTIVE_QUERY_STORAGE_PREFIX,
      });
    for (const [storageKey, value] of storedActiveQueries) {
      const activeQuery = this.parseStoredActiveQueryValue(value);
      if (!activeQuery) {
        continue;
      }

      const activeQueryKey = storageKey.slice(
        REALTIME_ACTIVE_QUERY_STORAGE_PREFIX.length
      );
      this.activeQueries.set(activeQueryKey, activeQuery);
      this.addToIndexes(activeQueryKey, activeQuery);
    }
    this.loaded = true;
  }

  async syncRegistrations(
    registrations: readonly StoredRealtimeRegistration[]
  ): Promise<void> {
    const desiredMembers = new Map<string, Set<string>>();
    for (const registration of registrations) {
      const registrationKey = this.registrationKey(registration);
      const activeQueryKey = this.keyForRegistration(
        registration,
        registrationKey
      );
      registration.activeQueryKey = activeQueryKey;
      const members = desiredMembers.get(activeQueryKey) ?? new Set<string>();
      members.add(registrationKey);
      desiredMembers.set(activeQueryKey, members);
      if (!this.activeQueries.has(activeQueryKey)) {
        await this.upsertFromRegistration(registrationKey, registration);
      }
    }

    for (const [activeQueryKey, members] of desiredMembers) {
      const activeQuery = this.activeQueries.get(activeQueryKey);
      if (!activeQuery) {
        continue;
      }

      await this.replaceMembers(activeQueryKey, activeQuery, members);
    }

    for (const activeQueryKey of Array.from(this.activeQueries.keys())) {
      if (!desiredMembers.has(activeQueryKey)) {
        await this.delete(activeQueryKey);
      }
    }
  }

  get(activeQueryKey: string): StoredRealtimeActiveQuery | undefined {
    return this.activeQueries.get(activeQueryKey);
  }

  values(): StoredRealtimeActiveQuery[] {
    return Array.from(this.activeQueries.values());
  }

  size(): number {
    return this.activeQueries.size;
  }

  maxFanout(): number {
    let maxFanout = 0;
    for (const activeQuery of this.activeQueries.values()) {
      maxFanout = Math.max(maxFanout, activeQuery.memberRegistrationKeys.size);
    }
    return maxFanout;
  }

  getRelevantKeys(targets: RealtimeAffectedTargets): Set<string> {
    if (targets.all) {
      return new Set(this.activeQueries.keys());
    }

    const activeQueryKeys = new Set(this.activeQueryKeysWithoutDependencies);
    for (const tableName of targets.tables) {
      this.addIndexedKeys(
        activeQueryKeys,
        this.activeQueryKeysByTable.get(tableName)
      );
    }

    for (const partitionId of targets.partitions) {
      this.addIndexedKeys(
        activeQueryKeys,
        this.activeQueryKeysByPartition.get(partitionId)
      );
    }

    for (const tableName of targets.broadTables) {
      this.addPartitionKeysForTable(activeQueryKeys, tableName);
    }

    return activeQueryKeys;
  }

  async upsertFromRegistration(
    registrationKey: string,
    registration: StoredRealtimeRegistration,
    options: { readonly recomputeKey?: boolean } = {}
  ): Promise<string> {
    const activeQueryKey = this.keyForRegistration(
      registration,
      registrationKey,
      {
        recompute: options.recomputeKey === true,
      }
    );
    const existing = this.activeQueries.get(activeQueryKey);
    const nextActiveQuery: StoredRealtimeActiveQuery = existing
      ? {
          ...existing,
          dependencies:
            options.recomputeKey === true
              ? registration.dependencies
              : existing.dependencies,
          lastResultJson:
            options.recomputeKey === true
              ? registration.lastResultJson
              : existing.lastResultJson,
          memberRegistrationKeys: new Set([
            ...existing.memberRegistrationKeys,
            registrationKey,
          ]),
          reEvaluationRetryAt:
            options.recomputeKey === true
              ? registration.reEvaluationRetryAt
              : existing.reEvaluationRetryAt,
          versionSnapshot:
            options.recomputeKey === true
              ? registration.versionSnapshot
              : existing.versionSnapshot,
        }
      : {
          args: registration.args,
          authorizationHeader: registration.authorizationHeader,
          dependencies: registration.dependencies,
          key: activeQueryKey,
          lastResultJson: registration.lastResultJson,
          memberRegistrationKeys: new Set([registrationKey]),
          queryName: registration.queryName,
          reEvaluationRetryAt: registration.reEvaluationRetryAt,
          runtimeId: registration.runtimeId,
          versionSnapshot: registration.versionSnapshot,
        };

    await this.persistByKey(activeQueryKey, nextActiveQuery);
    if (existing) {
      this.removeFromIndexes(activeQueryKey, existing);
    }
    this.activeQueries.set(activeQueryKey, nextActiveQuery);
    this.addToIndexes(activeQueryKey, nextActiveQuery);
    registration.activeQueryKey = activeQueryKey;
    return activeQueryKey;
  }

  async detachRegistration(
    registrationKey: string,
    activeQueryKey: string | undefined
  ): Promise<void> {
    if (!activeQueryKey) {
      return;
    }

    const activeQuery = this.activeQueries.get(activeQueryKey);
    if (!activeQuery) {
      return;
    }

    const memberRegistrationKeys = new Set(activeQuery.memberRegistrationKeys);
    memberRegistrationKeys.delete(registrationKey);
    if (memberRegistrationKeys.size === 0) {
      await this.delete(activeQueryKey);
      return;
    }

    await this.replaceMembers(
      activeQueryKey,
      activeQuery,
      memberRegistrationKeys
    );
  }

  async markBackedOff(
    activeQuery: StoredRealtimeActiveQuery,
    retryAt = Date.now() + REALTIME_REEVALUATION_FAILURE_RETRY_MS
  ): Promise<void> {
    const nextActiveQuery = { ...activeQuery, reEvaluationRetryAt: retryAt };
    await this.persistByKey(activeQuery.key, nextActiveQuery);
    activeQuery.reEvaluationRetryAt = retryAt;
  }

  async clearBackoff(activeQuery: StoredRealtimeActiveQuery): Promise<void> {
    const nextActiveQuery = {
      ...activeQuery,
      reEvaluationRetryAt: undefined,
    };
    await this.persistByKey(activeQuery.key, nextActiveQuery);
    activeQuery.reEvaluationRetryAt = undefined;
  }

  snapshotForTest(): RealtimeActiveQueryStoreSnapshot {
    return {
      activeQueries: this.activeQueries,
      activeQueryKeysByPartition: this.activeQueryKeysByPartition,
      activeQueryKeysByTable: this.activeQueryKeysByTable,
      activeQueryKeysWithoutDependencies:
        this.activeQueryKeysWithoutDependencies,
    };
  }

  private keyForRegistration(
    registration: StoredRealtimeRegistration,
    registrationKey: string,
    options: { readonly recompute?: boolean } = {}
  ): string {
    return (
      (options.recompute ? undefined : registration.activeQueryKey) ??
      createRealtimeActiveQueryKey(registration, registrationKey)
    );
  }

  private async replaceMembers(
    activeQueryKey: string,
    activeQuery: StoredRealtimeActiveQuery,
    memberRegistrationKeys: Set<string>
  ): Promise<void> {
    const nextActiveQuery = {
      ...activeQuery,
      memberRegistrationKeys,
    };
    await this.persistByKey(activeQueryKey, nextActiveQuery);
    activeQuery.memberRegistrationKeys.clear();
    for (const registrationKey of memberRegistrationKeys) {
      activeQuery.memberRegistrationKeys.add(registrationKey);
    }
  }

  private async delete(activeQueryKey: string): Promise<void> {
    const activeQuery = this.activeQueries.get(activeQueryKey);
    if (!activeQuery) {
      await this.state.storage?.delete?.(this.storageKey(activeQueryKey));
      return;
    }

    await this.state.storage?.delete?.(this.storageKey(activeQueryKey));
    this.removeFromIndexes(activeQueryKey, activeQuery);
    this.activeQueries.delete(activeQueryKey);
  }

  private async persistByKey(
    activeQueryKey: string,
    activeQuery: StoredRealtimeActiveQuery
  ): Promise<void> {
    await this.state.storage?.put?.(
      this.storageKey(activeQueryKey),
      this.serializeStoredActiveQuery(activeQuery)
    );
  }

  private storageKey(activeQueryKey: string): string {
    return `${REALTIME_ACTIVE_QUERY_STORAGE_PREFIX}${activeQueryKey}`;
  }

  private parseStoredActiveQueryValue(
    value: unknown
  ): StoredRealtimeActiveQuery | undefined {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return undefined;
    }

    const input = value as Record<string, unknown>;
    if (
      typeof input.key !== "string" ||
      typeof input.queryName !== "string" ||
      typeof input.runtimeId !== "string" ||
      !Array.isArray(input.memberRegistrationKeys)
    ) {
      return undefined;
    }

    return {
      args: input.args ?? {},
      authorizationHeader:
        typeof input.authorizationHeader === "string"
          ? input.authorizationHeader
          : undefined,
      dependencies: parseRealtimeDependencySetValue(input.dependencies),
      key: input.key,
      lastResultJson:
        typeof input.lastResultJson === "string"
          ? input.lastResultJson
          : undefined,
      memberRegistrationKeys: new Set(
        input.memberRegistrationKeys.filter(
          (registrationKey): registrationKey is string =>
            typeof registrationKey === "string"
        )
      ),
      queryName: input.queryName,
      reEvaluationRetryAt:
        typeof input.reEvaluationRetryAt === "number"
          ? input.reEvaluationRetryAt
          : undefined,
      runtimeId: input.runtimeId,
      versionSnapshot: parseRealtimeVersionSnapshotValue(input.versionSnapshot),
    };
  }

  private serializeStoredActiveQuery(
    activeQuery: StoredRealtimeActiveQuery
  ): StoredRealtimeActiveQueryValue {
    return {
      ...activeQuery,
      dependencies: activeQuery.dependencies
        ? serializeRealtimeDependencySet(activeQuery.dependencies)
        : undefined,
      memberRegistrationKeys: [...activeQuery.memberRegistrationKeys],
      versionSnapshot: activeQuery.versionSnapshot
        ? serializeRealtimeVersionSnapshot(activeQuery.versionSnapshot)
        : undefined,
    };
  }

  private addPartitionKeysForTable(
    target: Set<string>,
    tableName: string
  ): void {
    for (const [partitionId, activeQueryKeys] of this
      .activeQueryKeysByPartition) {
      if (getPartitionDependencyTable(partitionId) === tableName) {
        this.addIndexedKeys(target, activeQueryKeys);
      }
    }
  }

  private addIndexedKeys(
    target: Set<string>,
    activeQueryKeys: ReadonlySet<string> | undefined
  ): void {
    if (!activeQueryKeys) {
      return;
    }

    for (const activeQueryKey of activeQueryKeys) {
      target.add(activeQueryKey);
    }
  }

  private addToIndexes(
    activeQueryKey: string,
    activeQuery: StoredRealtimeActiveQuery
  ): void {
    if (
      !activeQuery.dependencies ||
      (activeQuery.dependencies.tables.size === 0 &&
        activeQuery.dependencies.partitions.size === 0)
    ) {
      this.activeQueryKeysWithoutDependencies.add(activeQueryKey);
      return;
    }

    for (const tableName of activeQuery.dependencies.tables) {
      this.addToIndex(this.activeQueryKeysByTable, tableName, activeQueryKey);
    }

    for (const partitionId of activeQuery.dependencies.partitions) {
      this.addToIndex(
        this.activeQueryKeysByPartition,
        partitionId,
        activeQueryKey
      );
    }
  }

  private addToIndex(
    index: Map<string, Set<string>>,
    indexKey: string,
    activeQueryKey: string
  ): void {
    const activeQueryKeys = index.get(indexKey) ?? new Set<string>();
    activeQueryKeys.add(activeQueryKey);
    index.set(indexKey, activeQueryKeys);
  }

  private removeFromIndexes(
    activeQueryKey: string,
    activeQuery: StoredRealtimeActiveQuery
  ): void {
    this.activeQueryKeysWithoutDependencies.delete(activeQueryKey);
    if (!activeQuery.dependencies) {
      return;
    }

    for (const tableName of activeQuery.dependencies.tables) {
      this.removeFromIndex(
        this.activeQueryKeysByTable,
        tableName,
        activeQueryKey
      );
    }

    for (const partitionId of activeQuery.dependencies.partitions) {
      this.removeFromIndex(
        this.activeQueryKeysByPartition,
        partitionId,
        activeQueryKey
      );
    }
  }

  private removeFromIndex(
    index: Map<string, Set<string>>,
    indexKey: string,
    activeQueryKey: string
  ): void {
    const activeQueryKeys = index.get(indexKey);
    if (!activeQueryKeys) {
      return;
    }

    activeQueryKeys.delete(activeQueryKey);
    if (activeQueryKeys.size === 0) {
      index.delete(indexKey);
    }
  }

  private registrationKey(registration: StoredRealtimeRegistration): string {
    return createRegistrationKey(
      registration.connectionKey,
      registration.subscriptionId
    );
  }
}
