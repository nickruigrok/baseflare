import { describe, expect, it } from "vitest";
import { FakeRealtimeDurableObjectState } from "../realtime.test-helpers";
import { RealtimeRegistrationStore } from "./registration-store";
import { createRegistrationKey } from "./routing";
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
        activeQueryKey: "active-query-a",
        dependencies: { partitions: new Set(), tables: new Set(["todos"]) },
      })
    );

    const reloadedStore = new RealtimeRegistrationStore(state);
    await reloadedStore.loadOnce();

    expect(reloadedStore.get(registrationKey("sub-a"))).toMatchObject({
      activeQueryKey: "active-query-a",
      queryName: "todos:list",
      subscriptionId: "sub-a",
    });
    expect(
      reloadedStore.get(registrationKey("sub-a"))?.dependencies?.tables
    ).toEqual(new Set(["todos"]));
    expect(reloadedStore.size()).toBe(1);
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

  it("cleans up only expired registrations", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeRegistrationStore(state);
    const expiredRegistration = {
      ...registration("sub-expired"),
      leaseExpiresAt: Date.now() - 1,
    };
    const activeRegistration = registration("sub-active");
    await store.upsert(registrationKey("sub-expired"), expiredRegistration);
    await store.upsert(registrationKey("sub-active"), activeRegistration);

    await store.cleanupExpired();

    expect(store.get(registrationKey("sub-expired"))).toBeUndefined();
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
});
