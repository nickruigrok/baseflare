import { logRuntimeEvent } from "../logging";
import type { RealtimeActiveQueryStore } from "./active-query-store";
import {
  createRegistrationKey,
  parseRealtimeDependencySetValue,
  parseRealtimeVersionSnapshotValue,
  serializeRealtimeDependencySet,
  serializeRealtimeVersionSnapshot,
} from "./routing";
import { getEpoch, getStringField } from "./shared";
import { listRealtimeStoragePrefix } from "./storage-list";
import type {
  RealtimeDependencySet,
  RealtimeDurableObjectState,
  RealtimeRegistration,
  RealtimeVersionSnapshot,
  StoredRealtimeRegistration,
} from "./types";
import {
  REALTIME_LEASE_MS,
  REALTIME_REEVALUATION_FAILURE_RETRY_MS,
} from "./types";

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

export class RealtimeRegistrationStore {
  private readonly registrations = new Map<
    string,
    StoredRealtimeRegistration
  >();
  private readonly activeQueryStore?: RealtimeActiveQueryStore;
  private readonly state: RealtimeDurableObjectState;
  private loaded = false;

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

    const storage = this.state.storage;
    if (!storage?.list) {
      this.loaded = true;
      return;
    }

    const storedRegistrations =
      await listRealtimeStoragePrefix<StoredRealtimeRegistrationValue>(
        storage,
        REALTIME_REGISTRATION_STORAGE_PREFIX
      );
    for (const [storageKey, value] of storedRegistrations) {
      const registration = this.parseStoredRegistrationValue(value);
      if (!registration) {
        logRuntimeEvent(
          "warn",
          "runtime.realtime_registration_reload_dropped",
          {
            storageKey,
          }
        );
        continue;
      }

      const registrationKey = storageKey.slice(
        REALTIME_REGISTRATION_STORAGE_PREFIX.length
      );
      this.registrations.set(registrationKey, registration);
    }
    await this.activeQueryStore?.syncRegistrations(this.values());
    this.loaded = true;
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
    await this.persistByKey(registrationKey, nextRegistration);
    registration.activeQueryKey = nextRegistration.activeQueryKey;
    this.registrations.set(registrationKey, nextRegistration);
    try {
      await this.activeQueryStore?.upsertFromRegistration(
        registrationKey,
        nextRegistration
      );
    } catch (error) {
      try {
        if (existing) {
          this.registrations.set(registrationKey, existing);
          registration.activeQueryKey = existing.activeQueryKey;
          await this.persistByKey(registrationKey, existing);
        } else {
          this.registrations.delete(registrationKey);
          registration.activeQueryKey = undefined;
          await this.deleteStoredByKey(registrationKey);
        }
      } catch (rollbackError) {
        logRuntimeEvent(
          "error",
          "runtime.realtime_registration_upsert_rollback_failed",
          {
            errorName:
              rollbackError instanceof Error
                ? rollbackError.name
                : typeof rollbackError,
            registrationKey,
          }
        );
      }
      throw error;
    }

    if (
      existing &&
      existing.activeQueryKey !== nextRegistration.activeQueryKey
    ) {
      await this.activeQueryStore?.detachRegistration(
        registrationKey,
        existing.activeQueryKey
      );
    }
  }

  async delete(registrationKey: string): Promise<void> {
    const registration = this.registrations.get(registrationKey);
    if (!registration) {
      await this.deleteStoredByKey(registrationKey);
      return;
    }

    await this.deleteStoredByKey(registrationKey);
    this.registrations.delete(registrationKey);
    await this.activeQueryStore?.detachRegistration(
      registrationKey,
      registration.activeQueryKey
    );
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

  async cleanupExpired(now = Date.now()): Promise<void> {
    const expiredRegistrationKeys: string[] = [];
    for (const [registrationKey, registration] of this.registrations) {
      if (registration.leaseExpiresAt <= now) {
        expiredRegistrationKeys.push(registrationKey);
      }
    }

    const results = await Promise.allSettled(
      expiredRegistrationKeys.map((registrationKey) =>
        this.delete(registrationKey)
      )
    );
    for (const [index, result] of results.entries()) {
      if (result.status === "fulfilled") {
        continue;
      }
      logRuntimeEvent("error", "runtime.realtime_registration_cleanup_failed", {
        errorName:
          result.reason instanceof Error
            ? result.reason.name
            : typeof result.reason,
        registrationKey: expiredRegistrationKeys[index],
      });
    }
  }

  async updateSameShardDependencies(
    registrationKey: string,
    registration: StoredRealtimeRegistration,
    dependencies: RealtimeDependencySet,
    versionSnapshot: RealtimeVersionSnapshot
  ): Promise<void> {
    const previousDependencies = registration.dependencies;
    const previousActiveQueryKey = registration.activeQueryKey;
    const previousVersionSnapshot = registration.versionSnapshot;
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

    await this.persistByKey(registrationKey, nextRegistration);
    registration.dependencies = dependencies;
    registration.versionSnapshot = versionSnapshot;
    registration.activeQueryKey = nextRegistration.activeQueryKey;
    this.registrations.set(registrationKey, registration);
    try {
      await this.activeQueryStore?.upsertFromRegistration(
        registrationKey,
        registration,
        { recomputeKey: true }
      );
    } catch (error) {
      registration.dependencies = previousDependencies;
      registration.versionSnapshot = previousVersionSnapshot;
      registration.activeQueryKey = previousActiveQueryKey;
      this.registrations.set(registrationKey, registration);
      try {
        await this.persistByKey(registrationKey, registration);
      } catch (rollbackError) {
        logRuntimeEvent(
          "error",
          "runtime.realtime_registration_dependency_update_rollback_failed",
          {
            errorName:
              rollbackError instanceof Error
                ? rollbackError.name
                : typeof rollbackError,
            registrationKey,
          }
        );
      }
      throw error;
    }
    if (previousActiveQueryKey !== registration.activeQueryKey) {
      await this.activeQueryStore?.detachRegistration(
        registrationKey,
        previousActiveQueryKey
      );
    }
  }

  async markBackedOff(
    registration: StoredRealtimeRegistration,
    retryAt = Date.now() + REALTIME_REEVALUATION_FAILURE_RETRY_MS
  ): Promise<void> {
    const registrationKey = createRegistrationKey(
      registration.connectionKey,
      registration.subscriptionId
    );
    await this.persistByKey(registrationKey, {
      ...registration,
      reEvaluationRetryAt: retryAt,
    });
    registration.reEvaluationRetryAt = retryAt;
    const storedRegistration = this.registrations.get(registrationKey);
    if (storedRegistration && storedRegistration !== registration) {
      storedRegistration.reEvaluationRetryAt = retryAt;
    }
  }

  async clearBackoff(registration: StoredRealtimeRegistration): Promise<void> {
    const nextRegistration = {
      ...registration,
      reEvaluationRetryAt: undefined,
    };

    await this.persist(nextRegistration);
    registration.reEvaluationRetryAt = undefined;
  }

  async renewLease(
    registration: StoredRealtimeRegistration,
    leaseExpiresAt: number
  ): Promise<void> {
    const nextRegistration = {
      ...registration,
      leaseExpiresAt,
      reEvaluationRetryAt: undefined,
    };

    await this.persist(nextRegistration);
    registration.leaseExpiresAt = leaseExpiresAt;
    registration.reEvaluationRetryAt = undefined;
  }

  async markDelivered(
    registration: StoredRealtimeRegistration,
    lastResultJson: string,
    leaseExpiresAt: number
  ): Promise<void> {
    const nextRegistration = {
      ...registration,
      lastResultJson,
      leaseExpiresAt,
      reEvaluationRetryAt: undefined,
    };

    await this.persist(nextRegistration);
    registration.lastResultJson = lastResultJson;
    registration.leaseExpiresAt = leaseExpiresAt;
    registration.reEvaluationRetryAt = undefined;
  }

  snapshotForTest(): RealtimeRegistrationStoreSnapshot {
    return {
      registrations: this.registrations,
    };
  }

  private async persist(
    registration: StoredRealtimeRegistration
  ): Promise<void> {
    await this.persistByKey(
      createRegistrationKey(
        registration.connectionKey,
        registration.subscriptionId
      ),
      registration
    );
  }

  private async persistByKey(
    registrationKey: string,
    registration: StoredRealtimeRegistration
  ): Promise<void> {
    await this.state.storage?.put?.(
      this.registrationStorageKey(registrationKey),
      this.serializeStoredRegistration(registration)
    );
  }

  private async deleteStoredByKey(registrationKey: string): Promise<void> {
    await this.state.storage?.delete?.(
      this.registrationStorageKey(registrationKey)
    );
  }

  private registrationStorageKey(registrationKey: string): string {
    return `${REALTIME_REGISTRATION_STORAGE_PREFIX}${registrationKey}`;
  }

  private parseStoredRegistrationValue(
    value: unknown
  ): StoredRealtimeRegistration | undefined {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return undefined;
    }

    try {
      const input = value as Record<string, unknown>;
      return {
        ...this.parseRegistration(input),
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
          typeof input.activeQueryKey === "string"
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

  private parseRegistration(
    body: Record<string, unknown>
  ): RealtimeRegistration {
    return {
      args: body.args ?? {},
      authorizationHeader:
        typeof body.authorizationHeader === "string"
          ? body.authorizationHeader
          : undefined,
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
