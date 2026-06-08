import { describe, expect, it } from "vitest";
import { FakeRealtimeDurableObjectState } from "../realtime.test-helpers";
import { RealtimeActiveQueryStore } from "./active-query-store";
import {
  createRealtimeAffectedTargets,
  createRegistrationKey,
} from "./routing";
import type {
  RealtimeDependencySet,
  RealtimePartitionTarget,
  RealtimeVersionSnapshot,
  StoredRealtimeRegistration,
} from "./types";

const leaseExpiresAt = () => Date.now() + 60_000;

function registration(
  subscriptionId: string,
  overrides: Partial<StoredRealtimeRegistration> = {}
): StoredRealtimeRegistration {
  return {
    args: { ownerToken: "owner-a" },
    authorizationHeader: "Bearer owner-a",
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

function todoOwnerPartition(ownerToken: string): RealtimePartitionTarget {
  return {
    partitionKey: "by_owner",
    partitionValue: JSON.stringify([ownerToken]),
    tableName: "todos",
  };
}

function todoOwnerPartitionId(ownerToken: string): string {
  const partition = todoOwnerPartition(ownerToken);
  return JSON.stringify([
    partition.tableName,
    partition.partitionKey,
    partition.partitionValue,
  ]);
}

function dependencies(tables: readonly string[] = []): RealtimeDependencySet {
  return {
    partitions: new Set(),
    tables: new Set(tables),
  };
}

function versionSnapshot(
  tables: readonly string[] = []
): RealtimeVersionSnapshot {
  return {
    partitions: new Map(),
    tables: new Map(tables.map((tableName) => [tableName, 1])),
  };
}

describe("RealtimeActiveQueryStore", () => {
  it("coalesces identical registrations into one active query", async () => {
    const store = new RealtimeActiveQueryStore(
      new FakeRealtimeDurableObjectState()
    );
    await store.upsertFromRegistration(
      registrationKey("sub-a"),
      registration("sub-a")
    );
    await store.upsertFromRegistration(
      registrationKey("sub-b"),
      registration("sub-b")
    );
    await store.upsertFromRegistration(
      registrationKey("sub-c"),
      registration("sub-c", { authorizationHeader: "Bearer owner-b" })
    );

    expect(store.size()).toBe(2);
    expect(store.maxFanout()).toBe(2);
    const sharedQuery = store
      .values()
      .find((activeQuery) => activeQuery.memberRegistrationKeys.size === 2);
    expect(sharedQuery?.memberRegistrationKeys).toEqual(
      new Set([registrationKey("sub-a"), registrationKey("sub-b")])
    );
  });

  it("loads persisted active queries and rebuilds dependency indexes", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeActiveQueryStore(state);
    const storedRegistration = registration("sub-a", {
      dependencies: dependencies(["todos"]),
      versionSnapshot: versionSnapshot(["todos"]),
    });
    await store.upsertFromRegistration(
      registrationKey("sub-a"),
      storedRegistration
    );

    const reloadedStore = new RealtimeActiveQueryStore(state);
    await reloadedStore.loadOnce();

    expect(
      reloadedStore.getRelevantKeys(
        createRealtimeAffectedTargets([
          {
            createdAt: Date.now(),
            eventId: "event-todos",
            partitions: [],
            sequence: 1,
            tables: ["todos"],
          },
        ])
      ).size
    ).toBe(1);
  });

  it("loads persisted active queries across storage list pages", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeActiveQueryStore(state);
    for (let index = 0; index < 150; index += 1) {
      const subscriptionId = `sub-${index.toString().padStart(3, "0")}`;
      await store.upsertFromRegistration(
        registrationKey(subscriptionId),
        registration(subscriptionId, {
          args: { ownerToken: `owner-${index}` },
          dependencies: dependencies(["todos"]),
          versionSnapshot: versionSnapshot(["todos"]),
        })
      );
    }

    const reloadedStore = new RealtimeActiveQueryStore(state);
    await reloadedStore.loadOnce();

    expect(reloadedStore.size()).toBe(150);
    expect(
      reloadedStore.getRelevantKeys(
        createRealtimeAffectedTargets([
          {
            createdAt: Date.now(),
            eventId: "event-todos",
            partitions: [],
            sequence: 1,
            tables: ["todos"],
          },
        ])
      ).size
    ).toBe(150);
  });

  it("keeps active query indexes unchanged when dependency persistence fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeActiveQueryStore(state);
    await store.upsertFromRegistration(
      registrationKey("sub-a"),
      registration("sub-a", {
        dependencies: dependencies(["todos"]),
        versionSnapshot: versionSnapshot(["todos"]),
      })
    );
    const activeQuery = store.values()[0];
    if (!activeQuery) {
      throw new Error("Expected active query");
    }
    state.failedStoragePuts = 1;

    await expect(
      store.upsertFromRegistration(
        registrationKey("sub-a"),
        {
          ...registration("sub-a", {
            activeQueryKey: activeQuery.key,
            dependencies: dependencies(["labels"]),
            lastResultJson: JSON.stringify([{ id: "1" }]),
            versionSnapshot: versionSnapshot(["labels"]),
          }),
        },
        { recomputeKey: true }
      )
    ).rejects.toThrow("Storage put failed");

    expect(
      store.getRelevantKeys(
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
    ).toContain(activeQuery.key);
    expect(
      store.getRelevantKeys(
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
    ).not.toContain(activeQuery.key);
    expect(activeQuery.lastResultJson).toBeUndefined();
  });

  it("keeps evaluated state unchanged when evaluation persistence fails", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeActiveQueryStore(state);
    await store.upsertFromRegistration(
      registrationKey("sub-a"),
      registration("sub-a", {
        dependencies: dependencies(["todos"]),
        lastResultJson: JSON.stringify([{ id: "old" }]),
        versionSnapshot: versionSnapshot(["todos"]),
      })
    );
    const activeQuery = store.values()[0];
    if (!activeQuery) {
      throw new Error("Expected active query");
    }
    state.failedStoragePuts = 1;

    await expect(
      store.markEvaluated(
        activeQuery,
        JSON.stringify([{ id: "new" }]),
        dependencies(["labels"]),
        versionSnapshot(["labels"])
      )
    ).rejects.toThrow("Storage put failed");

    expect(activeQuery.lastResultJson).toBe(JSON.stringify([{ id: "old" }]));
    expect(activeQuery.dependencies?.tables).toEqual(new Set(["todos"]));
    expect(activeQuery.versionSnapshot?.tables).toEqual(
      new Map([["todos", 1]])
    );
    expect(
      store.getRelevantKeys(
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
    ).toContain(activeQuery.key);
    expect(
      store.getRelevantKeys(
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
    ).not.toContain(activeQuery.key);
  });

  it("persists evaluated state before updating memory and indexes", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeActiveQueryStore(state);
    await store.upsertFromRegistration(
      registrationKey("sub-a"),
      registration("sub-a", {
        dependencies: dependencies(["todos"]),
        lastResultJson: JSON.stringify([{ id: "old" }]),
        versionSnapshot: versionSnapshot(["todos"]),
      })
    );
    const activeQuery = store.values()[0];
    if (!activeQuery) {
      throw new Error("Expected active query");
    }

    await store.markEvaluated(
      activeQuery,
      JSON.stringify([{ id: "new" }]),
      dependencies(["labels"]),
      versionSnapshot(["labels"])
    );

    const reloadedStore = new RealtimeActiveQueryStore(state);
    await reloadedStore.loadOnce();
    const reloadedActiveQuery = reloadedStore.get(activeQuery.key);

    expect(activeQuery.lastResultJson).toBe(JSON.stringify([{ id: "new" }]));
    expect(activeQuery.dependencies?.tables).toEqual(new Set(["labels"]));
    expect(reloadedActiveQuery?.lastResultJson).toBe(
      JSON.stringify([{ id: "new" }])
    );
    expect(reloadedActiveQuery?.dependencies?.tables).toEqual(
      new Set(["labels"])
    );
    expect(
      store.getRelevantKeys(
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
    ).toContain(activeQuery.key);
  });

  it("deletes empty active queries after the last member detaches", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeActiveQueryStore(state);
    const storedRegistration = registration("sub-a");
    const activeQueryKey = await store.upsertFromRegistration(
      registrationKey("sub-a"),
      storedRegistration
    );

    await store.detachRegistration(registrationKey("sub-a"), activeQueryKey);
    const reloadedStore = new RealtimeActiveQueryStore(state);
    await reloadedStore.loadOnce();

    expect(store.size()).toBe(0);
    expect(reloadedStore.size()).toBe(0);
  });

  it("syncs persisted registrations into active query membership", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeActiveQueryStore(state);
    const partitionDependencies: RealtimeDependencySet = {
      partitions: new Set([todoOwnerPartitionId("owner-a")]),
      tables: new Set(),
    };

    await store.syncRegistrations([
      registration("sub-a", { dependencies: partitionDependencies }),
      registration("sub-b", { dependencies: partitionDependencies }),
    ]);

    const activeQuery = store.values()[0];
    expect(store.size()).toBe(1);
    expect(store.maxFanout()).toBe(2);
    expect(activeQuery?.memberRegistrationKeys).toEqual(
      new Set([registrationKey("sub-a"), registrationKey("sub-b")])
    );
    expect(
      store.getRelevantKeys(
        createRealtimeAffectedTargets([
          {
            createdAt: Date.now(),
            eventId: "event-partition",
            partitions: [todoOwnerPartition("owner-a")],
            sequence: 1,
            tables: ["todos"],
          },
        ])
      ).size
    ).toBe(1);
  });

  it("removes active queries that have no synced registrations", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const store = new RealtimeActiveQueryStore(state);
    await store.upsertFromRegistration(
      registrationKey("sub-a"),
      registration("sub-a", {
        dependencies: dependencies(["todos"]),
        versionSnapshot: versionSnapshot(["todos"]),
      })
    );

    await store.syncRegistrations([]);
    const reloadedStore = new RealtimeActiveQueryStore(state);
    await reloadedStore.loadOnce();

    expect(store.size()).toBe(0);
    expect(reloadedStore.size()).toBe(0);
    expect(
      store.getRelevantKeys(
        createRealtimeAffectedTargets([
          {
            createdAt: Date.now(),
            eventId: "event-todos",
            partitions: [],
            sequence: 1,
            tables: ["todos"],
          },
        ])
      ).size
    ).toBe(0);
  });
});
