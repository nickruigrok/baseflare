import { describe, expect, it } from "vitest";
import { restrictSnapshotToDependencies } from "./evaluation";
import type { RealtimeDependencySet, RealtimeVersionSnapshot } from "./types";

function snapshot(
  tables: Record<string, number>,
  partitions: Record<string, number> = {}
): RealtimeVersionSnapshot {
  return {
    partitions: new Map(Object.entries(partitions)),
    tables: new Map(Object.entries(tables)),
  };
}

function dependencies(
  tables: readonly string[],
  partitions: readonly string[] = []
): RealtimeDependencySet {
  return {
    partitions: new Set(partitions),
    tables: new Set(tables),
  };
}

describe("restrictSnapshotToDependencies", () => {
  it("keeps table and partition entries present in both inputs", () => {
    const result = restrictSnapshotToDependencies(
      snapshot({ todos: 3 }, { p1: 7 }),
      dependencies(["todos"], ["p1"])
    );

    expect([...result.tables]).toEqual([["todos", 3]]);
    expect([...result.partitions]).toEqual([["p1", 7]]);
  });

  it("drops snapshot entries that are not observed dependencies", () => {
    const result = restrictSnapshotToDependencies(
      snapshot({ todos: 3, users: 9 }, { p1: 7, p2: 4 }),
      dependencies(["todos"], ["p2"])
    );

    expect([...result.tables]).toEqual([["todos", 3]]);
    expect([...result.partitions]).toEqual([["p2", 4]]);
  });

  it("omits newly discovered dependencies missing from the snapshot", () => {
    const result = restrictSnapshotToDependencies(
      snapshot({ todos: 3 }),
      dependencies(["todos", "users"], ["p1"])
    );

    expect([...result.tables]).toEqual([["todos", 3]]);
    expect(result.partitions.size).toBe(0);
  });

  it("returns an empty snapshot when the pre-execution snapshot is empty", () => {
    const result = restrictSnapshotToDependencies(
      snapshot({}),
      dependencies(["todos"], ["p1"])
    );

    expect(result.tables.size).toBe(0);
    expect(result.partitions.size).toBe(0);
  });

  it("returns an empty snapshot when there are no dependencies", () => {
    const result = restrictSnapshotToDependencies(
      snapshot({ todos: 3 }, { p1: 7 }),
      dependencies([])
    );

    expect(result.tables.size).toBe(0);
    expect(result.partitions.size).toBe(0);
  });

  it("returns fresh map instances", () => {
    const source = snapshot({ todos: 3 });
    const result = restrictSnapshotToDependencies(
      source,
      dependencies(["todos"])
    );

    expect(result.tables).not.toBe(source.tables);
    expect(result.partitions).not.toBe(source.partitions);
  });
});
