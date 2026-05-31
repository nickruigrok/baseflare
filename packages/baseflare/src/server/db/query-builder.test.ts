import { minIdForMs } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { decodeCursor } from "./cursor";
import { compareSqliteJsonValues, matchesFilter } from "./filters";
import { createQueryBuilder } from "./query-builder";

const UNIQUE_DOCUMENT_ERROR_PATTERN = /Expected exactly one document/;

describe("createQueryBuilder", () => {
  it("compiles equality shorthand filters", () => {
    const query = createQueryBuilder("todos")
      .filter({ completed: false, status: "active" })
      .order("desc")
      .limit(10) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM todos WHERE (json_extract(_data, '$.completed') IS ? AND json_extract(_data, '$.status') IS ?) ORDER BY _id DESC LIMIT ?",
      params: [0, "active", 10],
    });
  });

  it("compiles field operators", () => {
    const query = createQueryBuilder("users").filter({
      age: { gt: 18, lte: 65 },
      status: { in: ["active", "pending"] },
    }) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM users WHERE ((COALESCE(json_extract(_data, '$.age') > ?, 0) AND COALESCE(json_extract(_data, '$.age') <= ?, 0)) AND json_extract(_data, '$.status') IN (?, ?)) ORDER BY _id ASC",
      params: [18, 65, "active", "pending"],
    });
  });

  it("compiles logical filters", () => {
    const query = createQueryBuilder("users").filter({
      AND: [
        { status: "active" },
        { OR: [{ age: { gt: 18 } }, { verified: true }] },
      ],
      NOT: { archived: true },
    }) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM users WHERE ((json_extract(_data, '$.status') IS ? AND (COALESCE(json_extract(_data, '$.age') > ?, 0) OR json_extract(_data, '$.verified') IS ?)) AND (NOT (json_extract(_data, '$.archived') IS ?))) ORDER BY _id ASC",
      params: ["active", 18, 1, 1],
    });
  });

  it("ANDs multiple filter calls together", () => {
    const query = createQueryBuilder("todos")
      .filter({ a: 1 })
      .filter({ b: 2 }) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM todos WHERE (json_extract(_data, '$.a') IS ? AND json_extract(_data, '$.b') IS ?) ORDER BY _id ASC",
      params: [1, 2],
    });
  });

  it("treats null filters as nullish for optional fields", () => {
    const explicitNull = createQueryBuilder("todos").filter({
      deletedAt: null,
    }) as ReturnType<typeof createQueryBuilder>;
    const notNull = createQueryBuilder("todos").filter({
      deletedAt: { neq: null },
    }) as ReturnType<typeof createQueryBuilder>;
    const notDone = createQueryBuilder("todos").filter({
      completed: { neq: true },
    }) as ReturnType<typeof createQueryBuilder>;
    const inWithNull = createQueryBuilder("todos").filter({
      status: { in: [null, "active"] },
    }) as ReturnType<typeof createQueryBuilder>;

    expect(explicitNull.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM todos WHERE (json_type(_data, '$.deletedAt') IS NULL OR json_type(_data, '$.deletedAt') IS 'null') ORDER BY _id ASC",
      params: [],
    });
    expect(notNull.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM todos WHERE (json_type(_data, '$.deletedAt') IS NOT NULL AND json_type(_data, '$.deletedAt') IS NOT 'null') ORDER BY _id ASC",
      params: [],
    });
    expect(notDone.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM todos WHERE json_extract(_data, '$.completed') IS NOT ? ORDER BY _id ASC",
      params: [1],
    });
    expect(inWithNull.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM todos WHERE ((json_type(_data, '$.status') IS NULL OR json_type(_data, '$.status') IS 'null') OR json_extract(_data, '$.status') IN (?)) ORDER BY _id ASC",
      params: ["active"],
    });
  });

  it("orders by a json field with a stable _id tiebreak", () => {
    const query = createQueryBuilder("todos").order(
      "priority",
      "desc"
    ) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL().sql).toBe(
      "SELECT _id, _data FROM todos ORDER BY json_extract(_data, '$.priority') DESC, _id DESC"
    );
  });

  it("compares in-memory values with SQLite json_extract ordering", () => {
    expect(compareSqliteJsonValues(null, null)).toBe(0);
    expect(compareSqliteJsonValues(null, 1)).toBeLessThan(0);
    expect(compareSqliteJsonValues(undefined, "a")).toBeLessThan(0);
    expect(compareSqliteJsonValues(false, true)).toBeLessThan(0);
    expect(compareSqliteJsonValues(2, "1")).toBeLessThan(0);
    expect(compareSqliteJsonValues("10", "2")).toBeLessThan(0);

    expect(matchesFilter({ value: { gt: 2 } }, { value: "1" })).toBe(true);
    expect(matchesFilter({ value: { lt: "1" } }, { value: 2 })).toBe(true);
  });

  it("compares non-scalar values by their stored JSON shape", () => {
    expect(
      compareSqliteJsonValues({ z: 1, a: 2 }, { a: 2, z: 1 })
    ).toBeGreaterThan(0);
    expect(compareSqliteJsonValues({ $bytes: "user" }, { a: 1 })).toBeLessThan(
      0
    );
    expect(
      compareSqliteJsonValues(new Uint8Array([255]), new Uint8Array([1, 2, 3]))
    ).toBeLessThan(0);
  });

  it("collapses _createdAt ordering to _id", () => {
    const query = createQueryBuilder("todos").order(
      "_createdAt",
      "asc"
    ) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL().sql).toBe(
      "SELECT _id, _data FROM todos ORDER BY _id ASC"
    );
  });

  it("filters _id directly", () => {
    const query = createQueryBuilder("todos").filter({
      _id: { in: ["id-1", "id-2"] },
    }) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM todos WHERE _id IN (?, ?) ORDER BY _id ASC",
      params: ["id-1", "id-2"],
    });
  });

  it("limits in filters to a bounded number of values", () => {
    const values = Array.from({ length: 100 }, (_, index) => `id-${index}`);
    const query = createQueryBuilder("todos").filter({
      _id: { in: values },
    }) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL().params).toHaveLength(100);

    expect(() =>
      (
        createQueryBuilder("todos").filter({
          _id: { in: [...values, "id-100"] },
        }) as ReturnType<typeof createQueryBuilder>
      ).toSQL()
    ).toThrow(/must not contain more than 100 values/);
  });

  it("translates _createdAt range filters to _id boundaries", () => {
    const query = createQueryBuilder("todos").filter({
      _createdAt: { gte: 1000 },
    }) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM todos WHERE _id >= ? ORDER BY _id ASC",
      params: [minIdForMs(1000)],
    });
  });

  it("rejects unsupported _createdAt operators", () => {
    expect(() =>
      (
        createQueryBuilder("todos").filter({ _createdAt: 5 }) as ReturnType<
          typeof createQueryBuilder
        >
      ).toSQL()
    ).toThrow(/does not support equality/);

    expect(() =>
      (
        createQueryBuilder("todos").filter({
          _createdAt: { in: [5] },
        }) as ReturnType<typeof createQueryBuilder>
      ).toSQL()
    ).toThrow(/does not support in/);
  });

  it("rejects invalid filter shapes", () => {
    expect(() =>
      (
        createQueryBuilder("todos").filter({
          status: { contains: "active" },
        } as never) as ReturnType<typeof createQueryBuilder>
      ).toSQL()
    ).toThrow(/not a supported filter operator/);

    expect(() =>
      (
        createQueryBuilder("todos").filter({
          status: { in: [] },
        }) as ReturnType<typeof createQueryBuilder>
      ).toSQL()
    ).toThrow(/must not be empty/);

    expect(() =>
      (
        createQueryBuilder("todos").filter({ OR: [] }) as ReturnType<
          typeof createQueryBuilder
        >
      ).toSQL()
    ).toThrow(/non-empty array/);

    expect(() =>
      (
        createQueryBuilder("todos").filter({ "bad name": 1 }) as ReturnType<
          typeof createQueryBuilder
        >
      ).toSQL()
    ).toThrow(/must be "_id"/);
  });

  it("reports indexed paths for invalid nested logical filters", () => {
    expect(() =>
      matchesFilter(
        {
          AND: [{ status: { contains: "active" } }],
        } as never,
        { status: "active" }
      )
    ).toThrow(/filter\.AND\[0\]\.status\.contains/);

    expect(() =>
      matchesFilter(
        {
          OR: [{ status: "active" }, { age: { contains: 18 } }],
        } as never,
        { status: "inactive" }
      )
    ).toThrow(/filter\.OR\[1\]\.age\.contains/);
  });

  it("supports unique, count, and pagination against an executor", async () => {
    const documents = [
      { _id: "019078e5-d29f-7000-8000-000000000001", text: "a" },
      { _id: "019078e5-d29f-7000-8000-000000000002", text: "b" },
      { _id: "019078e5-d29f-7000-8000-000000000003", text: "c" },
    ];

    const query = createQueryBuilder("todos", {
      collect(builtQuery) {
        const limit = builtQuery.params.at(-1);
        return Promise.resolve(
          typeof limit === "number" ? documents.slice(0, limit) : documents
        );
      },
      count() {
        return Promise.resolve(documents.length);
      },
    });

    await expect(query.unique()).rejects.toThrow(UNIQUE_DOCUMENT_ERROR_PATTERN);
    await expect(query.count()).resolves.toBe(3);
    await expect(query.take(2)).resolves.toHaveLength(2);

    const firstPage = await query.paginate({ numItems: 2, cursor: null });
    expect(firstPage.page).toEqual(documents.slice(0, 2));
    expect(firstPage.isDone).toBe(false);

    const decoded = decodeCursor(firstPage.continueCursor, {
      field: "_id",
      direction: "asc",
    });
    expect(decoded.id).toBe(documents[1]?._id);
  });

  it("echoes the incoming cursor and marks done on an empty final page", async () => {
    const query = createQueryBuilder("todos", {
      collect() {
        return Promise.resolve([]);
      },
    });

    const result = await query.paginate({ numItems: 2, cursor: null });
    expect(result.page).toEqual([]);
    expect(result.isDone).toBe(true);
    expect(result.continueCursor).toBe("");
  });
});
