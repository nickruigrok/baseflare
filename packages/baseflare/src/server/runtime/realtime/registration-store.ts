import { logRuntimeEvent } from "../logging";
import type { RealtimeActiveQueryStore } from "./active-query-store";
import { isRealtimeActiveQueryKey } from "./evaluation-key";
import {
  createRegistrationKey,
  parseRealtimeDependencySetValue,
  parseRealtimeVersionSnapshotValue,
  serializeRealtimeDependencySet,
  serializeRealtimeVersionSnapshot,
} from "./routing";
import { parseRealtimeRegistration } from "./shared";
import {
  type RealtimeStorageBatchOperation,
  writeRealtimeStorageBatch,
} from "./storage-batch";
import { listRealtimeStoragePrefix } from "./storage-list";
import type {
  RealtimeDependencySet,
  RealtimeDurableObjectState,
  RealtimeVersionSnapshot,
  StoredRealtimeRegistration,
} from "./types";
import { REALTIME_REEVALUATION_FAILURE_RETRY_MS } from "./types";

const REALTIME_REGISTRATION_STORAGE_PREFIX = "realtime:registration:";

type StoredRealtimeRegistrationValue = Omit<
  StoredRealtimeRegistration,
  "dependencies" | "versionSnapshot"
> & {
  readonly dependencies?: {
    readonly partitions: readonly string[];
    readonly tables: readonly string[];
  };
  readonly versionSnapshot?: {
    readonly partitions: ReadonlyArray<readonly [string, number]>;
    readonly tables: ReadonlyArray<readonly [string, number]>;
  };
};

export interface RealtimeRegistrationStoreSnapshot {
  readonly registrations: Map<string, StoredRealtimeRegistration>;
}

/**
 * Owns subscriber registration state for one subscription shard. Write
 * contract: in-memory state changes only AFTER the storage write succeeds,
 * and field updates are applied to both the passed registration and the
 * stored instance when they differ, so references held across an upsert
 * (e.g. by in-flight deliveries) never diverge from the store.
 */
export class RealtimeRegistrationStore {
  private readonly registrations = new Map<
    string,
    StoredRealtimeRegistration
  >();
  private readonly activeQueryStore?: RealtimeActiveQueryStore;
  private readonly state: RealtimeDurableObjectState;
  private loaded = false;
  private loadPromise: Promise<void> | undefined;

  constructor(
    state: RealtimeDurableObjectState,
    activeQueryStore?: RealtimeActiveQueryStore
  ) {
    this.activeQueryStore = activeQueryStore;
    this.state = state;
  }

  async loadOnce(): Promise<void> {
    if (this.loaded) {
      return;
    }

    // Coalesce concurrent callers: input gates only close during storage
    // operations, so a fetch and an alarm can interleave at the load's
    // non-storage awaits and would otherwise both run the full load.
    this.loadPromise ??= this.performLoadOnce().catch((error: unknown) => {
      // A failed load must not poison future attempts.
      this.loadPromise = undefined;
      throw error;
    });
    await this.loadPromise;
  }

  private async performLoadOnce(): Promise<void> {
    const storage = this.state.storage;
    if (!storage) {
      this.loaded = true;
      return;
    }

    const storedRegistrations =
      await listRealtimeStoragePrefix<StoredRealtimeRegistrationValue>(
        storage,
        REALTIME_REGISTRATION_STORAGE_PREFIX
      );
    const activeQueryKeysBeforeSync = new Map<string, string | undefined>();
    for (const [storageKey, value] of storedRegistrations) {
      const registration = await this.parseStoredRegistrationValue(value);
      if (!registration) {
        logRuntimeEvent(
          "warn",
          "runtime.realtime_registration_reload_dropped",
          {
            storageKey,
          }
        );
        await this.deleteQuietly(storage, storageKey);
        continue;
      }

      const registrationKey = storageKey.slice(
        REALTIME_REGISTRATION_STORAGE_PREFIX.length
      );
      this.registrations.set(registrationKey, registration);
      activeQueryKeysBeforeSync.set(
        registrationKey,
        registration.activeQueryKey
      );
    }
    // Sync recomputes each registration's canonical active-query key; persist
    // any registration whose stored pointer was repaired (e.g. after a crash
    // mid-move or a dropped active-query entry).
    await this.activeQueryStore?.syncRegistrations(this.values());
    const repairedRegistrationKeys = [...activeQueryKeysBeforeSync]
      .filter(
        ([registrationKey, activeQueryKey]) =>
          this.registrations.get(registrationKey)?.activeQueryKey !==
          activeQueryKey
      )
      .map(([registrationKey]) => registrationKey);
    // Best-effort hygiene: memory is already repaired; a failed persist just
    // leaves the stale stored pointer to be repaired again on the next load
    // and must not fail this one.
    const repairResults = await Promise.allSettled(
      repairedRegistrationKeys.map((registrationKey) => {
        const registration = this.registrations.get(registrationKey);
        return registration
          ? this.persistByKey(registrationKey, registration)
          : Promise.resolve();
      })
    );
    for (const [index, result] of repairResults.entries()) {
      if (result.status === "rejected") {
        logRuntimeEvent("warn", "runtime.realtime_load_cleanup_failed", {
          errorName:
            result.reason instanceof Error
              ? result.reason.name
              : typeof result.reason,
          storageKey: repairedRegistrationKeys[index],
        });
      }
    }
    this.loaded = true;
  }

  private async deleteQuietly(
    storage: NonNullable<RealtimeDurableObjectState["storage"]>,
    storageKey: string
  ): Promise<void> {
    try {
      await storage.delete(storageKey);
    } catch (error) {
      // Best-effort hygiene: the entry is already excluded from memory; a
      // failed delete must not fail the load. The next load retries it.
      logRuntimeEvent("warn", "runtime.realtime_load_cleanup_failed", {
        errorName: error instanceof Error ? error.name : typeof error,
        storageKey,
      });
    }
  }

  get(registrationKey: string): StoredRealtimeRegistration | undefined {
    return this.registrations.get(registrationKey);
  }

  size(): number {
    return this.registrations.size;
  }

  values(): StoredRealtimeRegistration[] {
    return Array.from(this.registrations.values());
  }

  async upsert(
    registrationKey: string,
    registration: StoredRealtimeRegistration
  ): Promise<void> {
    const existing = this.registrations.get(registrationKey);
    const activeQueryKey = await this.activeQueryStore?.getRegistrationKey(
      registration,
      registrationKey
    );
    const nextRegistration =
      activeQueryKey || registration.activeQueryKey
        ? {
            ...registration,
            activeQueryKey: activeQueryKey ?? registration.activeQueryKey,
          }
        : registration;
    const activeQueryChange =
      await this.activeQueryStore?.prepareUpsertFromRegistration(
        registrationKey,
        nextRegistration
      );
    // The old active-query detach joins the same batch so a failed write can
    // never leave the registration attached to one entry in storage and
    // another in memory.
    const detachChange =
      existing && existing.activeQueryKey !== nextRegistration.activeQueryKey
        ? this.activeQueryStore?.prepareDetachRegistration(
            registrationKey,
            existing.activeQueryKey
          )
        : undefined;
    await writeRealtimeStorageBatch(this.state.storage, [
      this.putOperation(registrationKey, nextRegistration),
      ...(activeQueryChange?.operations ?? []),
      ...(detachChange?.operations ?? []),
    ]);
    registration.activeQueryKey = nextRegistration.activeQueryKey;
    this.registrations.set(registrationKey, nextRegistration);
    activeQueryChange?.apply();
    detachChange?.apply();
  }

  async delete(registrationKey: string): Promise<void> {
    const registration = this.registrations.get(registrationKey);
    if (!registration) {
      await this.deleteStoredByKey(registrationKey);
      return;
    }

    const detachChange = this.activeQueryStore?.prepareDetachRegistration(
      registrationKey,
      registration.activeQueryKey
    );
    await writeRealtimeStorageBatch(this.state.storage, [
      this.deleteOperation(registrationKey),
      ...(detachChange?.operations ?? []),
    ]);
    this.registrations.delete(registrationKey);
    detachChange?.apply();
  }

  async deleteExpired(
    registration: StoredRealtimeRegistration,
    now = Date.now()
  ): Promise<boolean> {
    if (registration.leaseExpiresAt > now) {
      return false;
    }

    await this.delete(
      createRegistrationKey(
        registration.connectionKey,
        registration.subscriptionId
      )
    );
    return true;
  }

  expired(now = Date.now()): StoredRealtimeRegistration[] {
    const expiredRegistrations: StoredRealtimeRegistration[] = [];
    for (const registration of this.registrations.values()) {
      if (registration.leaseExpiresAt <= now) {
        expiredRegistrations.push(registration);
      }
    }

    return expiredRegistrations;
  }

  async updateSameShardDependencies(
    registrationKey: string,
    registration: StoredRealtimeRegistration,
    dependencies: RealtimeDependencySet,
    versionSnapshot: RealtimeVersionSnapshot
  ): Promise<void> {
    const previousActiveQueryKey = registration.activeQueryKey;
    const nextRegistration = {
      ...registration,
      dependencies,
      versionSnapshot,
    };
    const activeQueryKey = await this.activeQueryStore?.getRegistrationKey(
      nextRegistration,
      registrationKey,
      { recomputeKey: true }
    );
    if (activeQueryKey) {
      nextRegistration.activeQueryKey = activeQueryKey;
    }

    const activeQueryChange =
      await this.activeQueryStore?.prepareUpsertFromRegistration(
        registrationKey,
        nextRegistration,
        { recomputeKey: true }
      );
    const detachChange =
      previousActiveQueryKey === nextRegistration.activeQueryKey
        ? undefined
        : this.activeQueryStore?.prepareDetachRegistration(
            registrationKey,
            previousActiveQueryKey
          );
    await writeRealtimeStorageBatch(this.state.storage, [
      this.putOperation(registrationKey, nextRegistration),
      ...(activeQueryChange?.operations ?? []),
      ...(detachChange?.operations ?? []),
    ]);
    registration.dependencies = dependencies;
    registration.versionSnapshot = versionSnapshot;
    registration.activeQueryKey = nextRegistration.activeQueryKey;
    this.registrations.set(registrationKey, registration);
    activeQueryChange?.apply();
    detachChange?.apply();
  }

  async markBackedOff(
    registration: StoredRealtimeRegistration,
    retryAt = Date.now() + REALTIME_REEVALUATION_FAILURE_RETRY_MS
  ): Promise<void> {
    await this.commitRegistrationFields(registration, {
      reEvaluationRetryAt: retryAt,
    });
  }

  async clearBackoff(registration: StoredRealtimeRegistration): Promise<void> {
    await this.commitRegistrationFields(registration, {
      reEvaluationRetryAt: undefined,
    });
  }

  async renewLease(
    registration: StoredRealtimeRegistration,
    leaseExpiresAt: number
  ): Promise<void> {
    await this.commitRegistrationFields(registration, {
      leaseExpiresAt,
      reEvaluationRetryAt: undefined,
    });
  }

  async markDelivered(
    registration: StoredRealtimeRegistration,
    lastResultJson: string,
    leaseExpiresAt: number
  ): Promise<void> {
    await this.commitRegistrationFields(registration, {
      lastResultJson,
      leaseExpiresAt,
      reEvaluationRetryAt: undefined,
    });
  }

  /**
   * Persists the field updates, then applies them to BOTH the passed
   * registration and the stored instance when the caller holds a different
   * (older) one — e.g. a delivery captured before an upsert replaced the
   * stored object. In-memory state never changes before the write succeeds.
   * Every key in `fields` is applied explicitly, so `undefined` values clear
   * the field on both instances.
   */
  private async commitRegistrationFields(
    registration: StoredRealtimeRegistration,
    fields: Partial<
      Pick<
        StoredRealtimeRegistration,
        "lastResultJson" | "leaseExpiresAt" | "reEvaluationRetryAt"
      >
    >
  ): Promise<void> {
    const registrationKey = createRegistrationKey(
      registration.connectionKey,
      registration.subscriptionId
    );
    await this.persistByKey(registrationKey, {
      ...registration,
      ...fields,
    });
    const storedRegistration = this.registrations.get(registrationKey);
    Object.assign(registration, fields);
    if (storedRegistration && storedRegistration !== registration) {
      Object.assign(storedRegistration, fields);
    }
  }

  snapshotForTest(): RealtimeRegistrationStoreSnapshot {
    return {
      registrations: this.registrations,
    };
  }

  private async persistByKey(
    registrationKey: string,
    registration: StoredRealtimeRegistration
  ): Promise<void> {
    await writeRealtimeStorageBatch(this.state.storage, [
      this.putOperation(registrationKey, registration),
    ]);
  }

  private async deleteStoredByKey(registrationKey: string): Promise<void> {
    await writeRealtimeStorageBatch(this.state.storage, [
      this.deleteOperation(registrationKey),
    ]);
  }

  private registrationStorageKey(registrationKey: string): string {
    return `${REALTIME_REGISTRATION_STORAGE_PREFIX}${registrationKey}`;
  }

  private putOperation(
    registrationKey: string,
    registration: StoredRealtimeRegistration
  ): RealtimeStorageBatchOperation {
    return {
      key: this.registrationStorageKey(registrationKey),
      type: "put",
      value: this.serializeStoredRegistration(registration),
    };
  }

  private deleteOperation(
    registrationKey: string
  ): RealtimeStorageBatchOperation {
    return {
      key: this.registrationStorageKey(registrationKey),
      type: "delete",
    };
  }

  private async parseStoredRegistrationValue(
    value: unknown
  ): Promise<StoredRealtimeRegistration | undefined> {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return undefined;
    }

    try {
      const input = value as Record<string, unknown>;
      return {
        ...(await parseRealtimeRegistration(input)),
        dependencies: parseRealtimeDependencySetValue(input.dependencies),
        lastResultJson:
          typeof input.lastResultJson === "string"
            ? input.lastResultJson
            : undefined,
        movePending: input.movePending === true,
        reEvaluationRetryAt:
          typeof input.reEvaluationRetryAt === "number"
            ? input.reEvaluationRetryAt
            : undefined,
        activeQueryKey:
          typeof input.activeQueryKey === "string" &&
          isRealtimeActiveQueryKey(input.activeQueryKey)
            ? input.activeQueryKey
            : undefined,
        versionSnapshot: parseRealtimeVersionSnapshotValue(
          input.versionSnapshot
        ),
      };
    } catch {
      return undefined;
    }
  }

  private serializeStoredRegistration(
    registration: StoredRealtimeRegistration
  ): StoredRealtimeRegistrationValue {
    return {
      ...registration,
      dependencies: registration.dependencies
        ? serializeRealtimeDependencySet(registration.dependencies)
        : undefined,
      versionSnapshot: registration.versionSnapshot
        ? serializeRealtimeVersionSnapshot(registration.versionSnapshot)
        : undefined,
    };
  }
}
