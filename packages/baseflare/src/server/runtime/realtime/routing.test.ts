import { describe, expect, it } from "vitest";
import { createRealtimeOutboxOperation } from "./outbox";
import {
  createRealtimeGlobalSubscriptionRouteTarget,
  createRealtimePartitionSubscriptionRouteTarget,
  createRealtimeTableSubscriptionRouteTarget,
  getRealtimeAffectedSubscriptionRouteTargets,
  getRealtimeConnectionShardName,
  getRealtimeSubscriptionShardName,
  isZeroRealtimeVersionSnapshot,
} from "./routing";
import { REALTIME_CONNECTION_SHARD_COUNT } from "./types";

function todoOwnerPartition(ownerToken: string): {
  readonly partitionKey: string;
  readonly partitionValue: string;
  readonly tableName: "todos";
} {
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

describe("realtime routing", () => {
  it("uses database time for realtime outbox timestamps", () => {
    const operation = createRealtimeOutboxOperation(
      {
        eventId: "db-time-event",
        partitions: [],
        tables: ["todos"],
      },
      1
    );

    expect(operation.params).toEqual([
      "db-time-event",
      JSON.stringify(["todos"]),
      JSON.stringify([]),
      1,
    ]);
    expect(operation.sql).toContain("julianday('now')");
  });

  it("classifies empty realtime version snapshots as unknown, not zero", () => {
    expect(
      isZeroRealtimeVersionSnapshot({
        partitions: new Map(),
        tables: new Map(),
      })
    ).toBe(false);
    expect(
      isZeroRealtimeVersionSnapshot({
        partitions: new Map([[todoOwnerPartitionId("owner-a"), 0]]),
        tables: new Map(),
      })
    ).toBe(true);
    expect(
      isZeroRealtimeVersionSnapshot({
        partitions: new Map(),
        tables: new Map([["todos", 1]]),
      })
    ).toBe(false);
  });

  it("keeps realtime connection shard routing deterministic and bounded", () => {
    const first = getRealtimeConnectionShardName("client-a");
    const second = getRealtimeConnectionShardName("client-a");
    const shardNumber = Number(first.split(":").at(1));
    const seenShardNames = new Set(
      Array.from({ length: 5000 }, (_value, index) =>
        getRealtimeConnectionShardName(`client-${index}`)
      )
    );

    expect(first).toBe(second);
    expect(first).toMatch(/^connection:\d+$/);
    expect(shardNumber).toBeGreaterThanOrEqual(0);
    expect(shardNumber).toBeLessThan(REALTIME_CONNECTION_SHARD_COUNT);
    expect(seenShardNames.size).toBe(REALTIME_CONNECTION_SHARD_COUNT);
  });

  it("keeps N=1 realtime subscription routes on the default generation shard", () => {
    expect(
      getRealtimeSubscriptionShardName(
        createRealtimeGlobalSubscriptionRouteTarget()
      )
    ).toBe("subscription:g1:0");
    expect(
      getRealtimeSubscriptionShardName(
        createRealtimeTableSubscriptionRouteTarget("todos")
      )
    ).toBe("subscription:g1:0");
    expect(
      getRealtimeSubscriptionShardName(
        createRealtimePartitionSubscriptionRouteTarget(
          todoOwnerPartition("owner-a")
        )
      )
    ).toBe("subscription:g1:0");
  });

  it("keeps future realtime subscription shard routing deterministic and bounded", () => {
    const shardCount = 32;
    const partitionRoute = createRealtimePartitionSubscriptionRouteTarget(
      todoOwnerPartition("owner-a")
    );
    const tableRoute = createRealtimeTableSubscriptionRouteTarget("todos");
    const first = getRealtimeSubscriptionShardName(partitionRoute, shardCount);
    const second = getRealtimeSubscriptionShardName(partitionRoute, shardCount);
    const tableShard = getRealtimeSubscriptionShardName(tableRoute, shardCount);
    const shardNumber = Number(first.split(":").at(2));

    expect(first).toBe(second);
    expect(first).toMatch(/^subscription:g1:\d+$/);
    expect(tableShard).toMatch(/^subscription:g1:\d+$/);
    expect(shardNumber).toBeGreaterThanOrEqual(0);
    expect(shardNumber).toBeLessThan(shardCount);
  });

  it("derives realtime subscription routes from affected tables and partitions", () => {
    const routes = getRealtimeAffectedSubscriptionRouteTargets({
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });

    expect(routes).toEqual([
      {
        type: "global",
      },
      {
        partition: todoOwnerPartition("owner-a"),
        type: "partition",
      },
      {
        tableName: "todos",
        type: "table",
      },
    ]);
  });
});
