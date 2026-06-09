import { describe, expect, it } from "vitest";
import { FakeRealtimeDurableObjectState } from "../realtime.test-helpers";
import { RealtimeActiveQueryStore } from "./active-query-store";
import { RealtimeRegistrationStore } from "./registration-store";
import {
  createRealtimeAffectedTargets,
  createRegistrationKey,
} from "./routing";
import type { StoredRealtimeRegistration } from "./types";

const leaseExpiresAt = () => Date.now() + 60_000;

function registration(
  subscriptionId: string,
  overrides: Partial<StoredRealtimeRegistration> = {}
): StoredRealtimeRegistration {
  return {
    args: {},
    connectionKey: "client:client-a",
    connectionName: "connection:0",
    epoch: 1,
    leaseExpiresAt: leaseExpiresAt(),
    queryName: "todos:list",
    runtimeId: "runtime:1",
    subscriptionId,
    ...overrides,
  };
}

function registrationKey(subscriptionId: string): string {
  return createRegistrationKey("client:client-a", subscriptionId);
}

describe("RealtimeRegistrationStore", () => {
  it("loads persisted registrations", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeRegistrationStore(state);
    await store.upsert(
      registrationKey("sub-a"),
      registration("sub-a", {
        dependencies: { partitions: new Set(), tables: new Set(["todos"]) },
      })
    );

    const reloadedStore = new RealtimeRegistrationStore(state);
    await reloadedStore.loadOnce();

    expect(reloadedStore.get(registrationKey("sub-a"))).toMatchObject({
      queryName: "todos:list",
      subscriptionId: "sub-a",
    });
    expect(
      reloadedStore.get(registrationKey("sub-a"))?.dependencies?.tables
    ).toEqual(new Set(["todos"]));
    expect(reloadedStore.size()).toBe(1);
  });

  it("sanitizes legacy raw authorization headers when registrations reload", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const key = registrationKey("sub-a");
    await state.storage.put(`realtime:registration:${key}`, {
      ...registration("sub-a"),
      authorizationHeader: "Bearer owner-a",
    });

    const reloadedStore = new RealtimeRegistrationStore(state);
    await reloadedStore.loadOnce();

    const reloaded = reloadedStore.get(key);
    expect(reloaded?.authorizationFingerprint).toMatch(/^[0-9a-f]{64}$/);
    expect(JSON.stringify([...state.durableStorage.values()])).not.toContain(
      "Bearer owner-a"
    );
  });

  it("replaces legacy active query keys during registration reload", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const activeQueryStore = new RealtimeActiveQueryStore(state);
    const key = registrationKey("sub-a");
    await state.storage.put(`realtime:registration:${key}`, {
      ...registration("sub-a"),
      activeQueryKey: "legacy-active-query-key",
    });

    const reloadedStore = new RealtimeRegistrationStore(
      state,
      activeQueryStore
    );
    await reloadedStore.loadOnce();

    const reloaded = reloadedStore.get(key);
    expect(reloaded?.activeQueryKey).toMatch(/^aq:/);
    expect(activeQueryStore.get(reloaded?.activeQueryKey ?? "")).toBeDefined();
    expect(JSON.stringify([...state.durableStorage.values()])).not.toContain(
      "legacy-active-query-key"
    );
  });

  it("loads persisted registrations across storage list pages", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeRegistrationStore(state);
    for (let index = 0; index < 150; index += 1) {
      const subscriptionId = `sub-${index.toString().padStart(3, "0")}`;
      await store.upsert(registrationKey(subscriptionId), {
        ...registration(subscriptionId),
        activeQueryKey: `active-query-${index}`,
      });
    }

    const reloadedStore = new RealtimeRegistrationStore(state);
    await reloadedStore.loadOnce();

    expect(reloadedStore.size()).toBe(150);
    expect(reloadedStore.get(registrationKey("sub-000"))).toMatchObject({
      subscriptionId: "sub-000",
    });
    expect(reloadedStore.get(registrationKey("sub-149"))).toMatchObject({
      subscriptionId: "sub-149",
    });
  });

  it("honors fake storage list limits and startAfter keys", async () => {
    const state = new FakeRealtimeDurableObjectState();
    await state.storage.put("test:001", 1);
    await state.storage.put("test:002", 2);
    await state.storage.put("test:003", 3);

    const firstPage = await state.storage.list<number>({
      limit: 2,
      prefix: "test:",
    });
    const secondPage = await state.storage.list<number>({
      limit: 2,
      prefix: "test:",
      startAfter: "test:002",
    });

    expect([...firstPage.keys()]).toEqual(["test:001", "test:002"]);
    expect([...secondPage.keys()]).toEqual(["test:003"]);
  });

  it("keeps memory unchanged when dependency persistence fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeRegistrationStore(state);
    const key = registrationKey("sub-a");
    const storedRegistration = registration("sub-a", {
      dependencies: { partitions: new Set(), tables: new Set(["todos"]) },
    });
    await store.upsert(key, storedRegistration);
    state.failedStoragePuts = 1;

    await expect(
      store.updateSameShardDependencies(
        key,
        storedRegistration,
        { partitions: new Set(), tables: new Set(["labels"]) },
        { partitions: new Map(), tables: new Map([["labels", 1]]) }
      )
    ).rejects.toThrow("Storage put failed");

    expect(storedRegistration.dependencies?.tables.has("todos")).toBe(true);
    expect(storedRegistration.dependencies?.tables.has("labels")).toBe(false);
  });

  it("does not update active query membership when registration persistence fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const activeQueryStore = new RealtimeActiveQueryStore(state);
    const store = new RealtimeRegistrationStore(state, activeQueryStore);
    const key = registrationKey("sub-a");
    state.failedStoragePuts = 1;

    await expect(store.upsert(key, registration("sub-a"))).rejects.toThrow(
      "Storage put failed"
    );

    expect(store.size()).toBe(0);
    expect(activeQueryStore.size()).toBe(0);
  });

  it("keeps registration and active query state unchanged when an upsert batch fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const activeQueryStore = new RealtimeActiveQueryStore(state);
    const store = new RealtimeRegistrationStore(state, activeQueryStore);
    const key = registrationKey("sub-a");
    const storedRegistration = registration("sub-a");
    state.failedStoragePuts = 1;

    await expect(store.upsert(key, storedRegistration)).rejects.toThrow(
      "Storage put failed"
    );

    expect(store.size()).toBe(0);
    expect(store.get(key)).toBeUndefined();
    expect(storedRegistration.activeQueryKey).toBeUndefined();
    expect(activeQueryStore.size()).toBe(0);
    expect(
      [...state.durableStorage.keys()].some((storageKey) =>
        storageKey.startsWith("realtime:registration:")
      )
    ).toBe(false);
  });

  it("keeps the previous registration when a replacement batch fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const activeQueryStore = new RealtimeActiveQueryStore(state);
    const store = new RealtimeRegistrationStore(state, activeQueryStore);
    const key = registrationKey("sub-a");
    const previousRegistration = registration("sub-a", {
      args: { filter: "old" },
    });
    await store.upsert(key, previousRegistration);
    const previousActiveQueryKey = previousRegistration.activeQueryKey;
    state.failedStoragePuts = 1;

    await expect(
      store.upsert(
        key,
        registration("sub-a", {
          args: { filter: "new" },
        })
      )
    ).rejects.toThrow("Storage put failed");

    expect(store.get(key)).toMatchObject({
      activeQueryKey: previousActiveQueryKey,
      args: { filter: "old" },
    });
    expect(activeQueryStore.size()).toBe(1);
    expect(
      previousActiveQueryKey
        ? activeQueryStore
            .get(previousActiveQueryKey)
            ?.memberRegistrationKeys.has(key)
        : false
    ).toBe(true);
  });

  it("keeps active query indexes unchanged when dependency registration persistence fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const activeQueryStore = new RealtimeActiveQueryStore(state);
    const store = new RealtimeRegistrationStore(state, activeQueryStore);
    const key = registrationKey("sub-a");
    const storedRegistration = registration("sub-a", {
      dependencies: { partitions: new Set(), tables: new Set(["todos"]) },
    });
    await store.upsert(key, storedRegistration);
    const activeQueryKey = storedRegistration.activeQueryKey;
    if (!activeQueryKey) {
      throw new Error("Expected active query key");
    }
    state.failedStoragePuts = 1;

    await expect(
      store.updateSameShardDependencies(
        key,
        storedRegistration,
        { partitions: new Set(), tables: new Set(["labels"]) },
        { partitions: new Map(), tables: new Map([["labels", 1]]) }
      )
    ).rejects.toThrow("Storage put failed");

    expect(
      activeQueryStore.getRelevantKeys(
        createRealtimeAffectedTargets([
          {
            createdAt: Date.now(),
            eventId: "event-todos",
            partitions: [],
            sequence: 1,
            tables: ["todos"],
          },
        ])
      )
    ).toContain(activeQueryKey);
    expect(
      activeQueryStore.getRelevantKeys(
        createRealtimeAffectedTargets([
          {
            createdAt: Date.now(),
            eventId: "event-labels",
            partitions: [],
            sequence: 2,
            tables: ["labels"],
          },
        ])
      )
    ).not.toContain(activeQueryKey);
  });

  it("keeps dependency state unchanged when a dependency update batch fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const activeQueryStore = new RealtimeActiveQueryStore(state);
    const store = new RealtimeRegistrationStore(state, activeQueryStore);
    const key = registrationKey("sub-a");
    const storedRegistration = registration("sub-a", {
      dependencies: { partitions: new Set(), tables: new Set(["todos"]) },
      versionSnapshot: {
        partitions: new Map(),
        tables: new Map([["todos", 1]]),
      },
    });
    await store.upsert(key, storedRegistration);
    const activeQueryKey = storedRegistration.activeQueryKey;
    if (!activeQueryKey) {
      throw new Error("Expected active query key");
    }
    state.failedStoragePuts = 1;

    await expect(
      store.updateSameShardDependencies(
        key,
        storedRegistration,
        { partitions: new Set(), tables: new Set(["labels"]) },
        { partitions: new Map(), tables: new Map([["labels", 1]]) }
      )
    ).rejects.toThrow("Storage put failed");

    expect(storedRegistration.activeQueryKey).toBe(activeQueryKey);
    expect(storedRegistration.dependencies?.tables).toEqual(new Set(["todos"]));
    expect(storedRegistration.versionSnapshot?.tables).toEqual(
      new Map([["todos", 1]])
    );
    expect(store.get(key)?.dependencies?.tables).toEqual(new Set(["todos"]));
    expect(
      activeQueryStore.getRelevantKeys(
        createRealtimeAffectedTargets([
          {
            createdAt: Date.now(),
            eventId: "event-todos",
            partitions: [],
            sequence: 1,
            tables: ["todos"],
          },
        ])
      )
    ).toContain(activeQueryKey);
    expect(
      activeQueryStore.getRelevantKeys(
        createRealtimeAffectedTargets([
          {
            createdAt: Date.now(),
            eventId: "event-labels",
            partitions: [],
            sequence: 2,
            tables: ["labels"],
          },
        ])
      )
    ).not.toContain(activeQueryKey);

    const reloadedStore = new RealtimeRegistrationStore(state);
    await reloadedStore.loadOnce();
    expect(reloadedStore.get(key)?.dependencies?.tables).toEqual(
      new Set(["todos"])
    );
  });

  it("updates dependency state after persistence succeeds", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeRegistrationStore(state);
    const key = registrationKey("sub-a");
    const storedRegistration = registration("sub-a", {
      dependencies: { partitions: new Set(), tables: new Set(["todos"]) },
    });
    await store.upsert(key, storedRegistration);

    await store.updateSameShardDependencies(
      key,
      storedRegistration,
      { partitions: new Set(), tables: new Set(["labels"]) },
      { partitions: new Map(), tables: new Map([["labels", 1]]) }
    );

    const reloadedStore = new RealtimeRegistrationStore(state);
    await reloadedStore.loadOnce();

    expect(storedRegistration.dependencies?.tables).toEqual(
      new Set(["labels"])
    );
    expect(reloadedStore.get(key)?.dependencies?.tables).toEqual(
      new Set(["labels"])
    );
  });

  it("deletes registrations from storage", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeRegistrationStore(state);
    const key = registrationKey("sub-a");
    await store.upsert(
      key,
      registration("sub-a", {
        dependencies: { partitions: new Set(), tables: new Set(["todos"]) },
      })
    );

    await store.delete(key);
    const reloadedStore = new RealtimeRegistrationStore(state);
    await reloadedStore.loadOnce();

    expect(reloadedStore.values()).toEqual([]);
  });

  it("lists only expired registrations", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeRegistrationStore(state);
    const expiredRegistration = {
      ...registration("sub-expired"),
      leaseExpiresAt: Date.now() - 1,
    };
    const activeRegistration = registration("sub-active");
    await store.upsert(registrationKey("sub-expired"), expiredRegistration);
    await store.upsert(registrationKey("sub-active"), activeRegistration);

    const expired = store.expired();

    expect(expired).toHaveLength(1);
    expect(expired[0]?.subscriptionId).toBe("sub-expired");
    expect(store.get(registrationKey("sub-active"))).toBe(activeRegistration);
  });

  it("does not advance delivered memory when delivery persistence fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeRegistrationStore(state);
    const key = registrationKey("sub-a");
    const storedRegistration = registration("sub-a");
    await store.upsert(key, storedRegistration);
    state.failedStoragePuts = 1;

    await expect(
      store.markDelivered(
        storedRegistration,
        JSON.stringify([{ id: "1" }]),
        leaseExpiresAt()
      )
    ).rejects.toThrow("Storage put failed");

    expect(storedRegistration.lastResultJson).toBeUndefined();
  });

  it("does not advance backoff memory when backoff persistence fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeRegistrationStore(state);
    const key = registrationKey("sub-a");
    const storedRegistration = registration("sub-a");
    const passedRegistration = { ...storedRegistration };
    await store.upsert(key, storedRegistration);
    state.failedStoragePuts = 1;

    await expect(
      store.markBackedOff(passedRegistration, Date.now() + 10_000)
    ).rejects.toThrow("Storage put failed");

    expect(passedRegistration.reEvaluationRetryAt).toBeUndefined();
    expect(store.get(key)?.reEvaluationRetryAt).toBeUndefined();

    const retryAt = Date.now() + 20_000;
    await store.markBackedOff(passedRegistration, retryAt);

    const reloadedStore = new RealtimeRegistrationStore(state);
    await reloadedStore.loadOnce();

    expect(passedRegistration.reEvaluationRetryAt).toBe(retryAt);
    expect(store.get(key)?.reEvaluationRetryAt).toBe(retryAt);
    expect(reloadedStore.get(key)?.reEvaluationRetryAt).toBe(retryAt);
  });
});
