import { v } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { createBaseQueryState } from "../db/query-builder";
import { defineRules } from "../permissions/define-rules";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import type { Schema } from "../schema/types";
import {
  assertKnownTable,
  assertWithinScanBudget,
  buildRuntimeSelectQuery,
  D1DatabaseAdapter,
  getNextRuntimeScanPosition,
  getRuntimeScanQueryOptions,
  type RuntimeScanPosition,
} from "./d1";
import type { D1Database, D1PreparedStatement, D1Result } from "./types";

class FakePreparedStatement implements D1PreparedStatement {
  private readonly firstResult: Record<string, unknown> | null;
  private readonly rows: readonly Record<string, unknown>[];
  readonly query: string;

  constructor(
    query: string,
    firstResult: Record<string, unknown> | null,
    rows: readonly Record<string, unknown>[]
  ) {
    this.firstResult = firstResult;
    this.rows = rows;
    this.query = query;
  }

  all<TRow = Record<string, unknown>>(): Promise<D1Result<TRow>> {
    const result = {
      results: this.query.includes("_bf_table_versions")
        ? [{ version: 0 }]
        : this.rows,
      success: true,
    } as unknown as D1Result<TRow>;
    return Promise.resolve(result);
  }

  bind(): D1PreparedStatement {
    return this;
  }

  first<TRow = Record<string, unknown>>(): Promise<TRow | null>;
  first<TRow extends Record<string, unknown>, K extends keyof TRow>(
    columnName: K
  ): Promise<TRow[K] | null>;
  first(): Promise<unknown> {
    return Promise.resolve(this.firstResult);
  }

  run(): Promise<D1Result> {
    return Promise.resolve({ success: true });
  }
}

const testId = "019078e5-d29f-7000-8000-000000000001";
const nextTestId = "019078e5-d29f-7000-8000-000000000002";

function createStoredRow(
  id = testId,
  data: Record<string, unknown> = { text: "before" }
) {
  return {
    _id: id,
    _data: JSON.stringify(data),
    _rev: 0,
  };
}

function createFakeDatabase(options: {
  readonly batchQueries?: string[][];
  readonly batchResults: readonly D1Result[];
  readonly queryRows?: readonly Record<string, unknown>[];
}): D1Database {
  return {
    batch(statements) {
      options.batchQueries?.push(
        statements.map((statement) =>
          statement instanceof FakePreparedStatement ? statement.query : ""
        )
      );
      return Promise.resolve(options.batchResults);
    },
    prepare(query) {
      return new FakePreparedStatement(
        query,
        {
          _id: testId,
          _data: JSON.stringify({ text: "before" }),
          _rev: 0,
        },
        options.queryRows ?? []
      );
    },
  };
}

function createTodoSchema() {
  return defineSchema({
    todos: defineTable({
      text: v.string(),
    }),
  });
}

function createAdapter(options?: {
  readonly batchQueries?: string[][];
  readonly batchResults?: readonly D1Result[];
  readonly queryRows?: readonly Record<string, unknown>[];
  readonly rules?: ReturnType<typeof defineRules>;
}): D1DatabaseAdapter {
  return new D1DatabaseAdapter({
    database: createFakeDatabase({
      batchQueries: options?.batchQueries,
      batchResults: options?.batchResults ?? [],
      queryRows: options?.queryRows,
    }),
    getContext: () => ({}),
    rules: options?.rules,
    schema: createTodoSchema(),
  });
}

describe("D1 runtime helpers", () => {
  it("looks up schema tables by own property only", () => {
    const schema = {
      tables: {},
      toCreateStatements: () => [],
    } as unknown as Schema;

    expect(() => assertKnownTable(schema, "constructor")).toThrow(
      'Unknown table "constructor"'
    );
  });

  it("fails clearly when internal scan budgets are exceeded", () => {
    expect(() => assertWithinScanBudget(20_001, 0)).toThrow(
      "Query exceeded the internal scan budget; add a more selective filter"
    );
    expect(() => assertWithinScanBudget(1, 5_000_001)).toThrow(
      "Query exceeded the internal scan budget; add a more selective filter"
    );
  });

  it("uses count-specific scan budget diagnostics", async () => {
    const database = createAdapter({
      queryRows: Array.from({ length: 20_001 }, (_, index) =>
        createStoredRow(
          `019078e5-d29f-7000-8000-${index.toString(16).padStart(12, "0")}`
        )
      ),
      rules: defineRules({
        todos: {
          read: () => true,
        },
      }),
    });

    await expect(database.query("todos").count()).rejects.toThrow(
      "Count exceeded the internal scan budget; add a more selective filter before count()"
    );
  });

  it("throws visibly when read rules are missing", async () => {
    const database = createAdapter({
      queryRows: [createStoredRow(testId)],
    });

    await expect(database.get("todos", testId)).rejects.toThrow(
      "Read rules are not configured"
    );
    await expect(database.query("todos").collect()).rejects.toThrow(
      "Read rules are not configured"
    );
  });

  it("keeps configured denied reads filtered", async () => {
    const database = createAdapter({
      queryRows: [createStoredRow(testId)],
      rules: defineRules({
        todos: {
          read: () => false,
        },
      }),
    });

    await expect(database.get("todos", testId)).resolves.toBeNull();
    await expect(database.query("todos").collect()).resolves.toEqual([]);
    await expect(database.query("todos").count()).resolves.toBe(0);
  });

  it("validates runtime select table identifiers", () => {
    expect(() =>
      buildRuntimeSelectQuery("bad table", createBaseQueryState(), {
        limit: 1,
      })
    ).toThrow(/must start with a letter/);
  });

  it("uses keyset predicates for id-ordered runtime scans", () => {
    const state = createBaseQueryState();
    const position = getNextRuntimeScanPosition(
      "todos",
      state,
      { cursor: null, offset: 0 },
      [createStoredRow(nextTestId)]
    );
    const query = buildRuntimeSelectQuery("todos", state, {
      cursor: position.cursor,
      limit: 256,
    });

    expect(query.sql).toContain("_id > ?");
    expect(query.sql).not.toContain("OFFSET");
    expect(query.params).toEqual([nextTestId, 256]);
  });

  it("uses keyset predicates for scalar field-ordered runtime scans", () => {
    const state = {
      ...createBaseQueryState(),
      order: { field: "text", direction: "asc" } as const,
    };
    const position = getNextRuntimeScanPosition(
      "todos",
      state,
      { cursor: null, offset: 0 },
      [createStoredRow(nextTestId, { text: "beta" })]
    );
    const query = buildRuntimeSelectQuery("todos", state, {
      cursor: position.cursor,
      limit: 256,
    });

    expect(query.sql).toContain(
      "(json_extract(_data, '$.text') > ? OR (json_extract(_data, '$.text') = ? AND _id > ?))"
    );
    expect(query.sql).not.toContain("OFFSET");
    expect(query.params).toEqual(["beta", "beta", nextTestId, 256]);
  });

  it("falls back to offsets for non-scalar field-ordered runtime scans", () => {
    const state = {
      ...createBaseQueryState(),
      order: { field: "text", direction: "asc" } as const,
    };
    const position: RuntimeScanPosition = getNextRuntimeScanPosition(
      "todos",
      state,
      { cursor: null, offset: 0 },
      [createStoredRow(nextTestId, { text: { nested: true } })]
    );
    const query = buildRuntimeSelectQuery("todos", state, {
      cursor: position.cursor,
      limit: 256,
      offset: position.cursor ? undefined : position.offset,
    });

    expect(position.cursor).toBeNull();
    expect(query.sql).toContain("OFFSET ?");
    expect(query.params).toEqual([256, 1]);
  });

  it("preserves the base cursor when runtime scans fall back to offsets", () => {
    const state = {
      ...createBaseQueryState(),
      order: { field: "text", direction: "asc" } as const,
    };
    const baseCursor = {
      id: testId,
      orderDirection: "asc",
      orderField: "text",
      v: "alpha",
    } as const;
    const position = getNextRuntimeScanPosition(
      "todos",
      state,
      { cursor: baseCursor, offset: 0 },
      [createStoredRow(nextTestId, { text: { nested: true } })]
    );
    const query = buildRuntimeSelectQuery("todos", state, {
      ...getRuntimeScanQueryOptions(position, baseCursor),
      limit: 256,
    });

    expect(position.cursor).toBeNull();
    expect(query.sql).toContain(
      "(json_extract(_data, '$.text') > ? OR (json_extract(_data, '$.text') = ? AND _id > ?))"
    );
    expect(query.sql).toContain("OFFSET ?");
    expect(query.params).toEqual(["alpha", "alpha", testId, 256, 1]);
  });

  it("includes the table name in duplicate unique errors", async () => {
    const database = createAdapter({
      queryRows: [
        createStoredRow(testId),
        createStoredRow(nextTestId, { text: "after" }),
      ],
      rules: defineRules({
        todos: {
          read: () => true,
        },
      }),
    });

    await expect(database.query("todos").unique()).rejects.toThrow(
      'Expected exactly one document from "todos", received 2'
    );
  });

  it("requires D1 change counts for direct write operations", async () => {
    const rules = defineRules({
      todos: {
        update: () => true,
      },
    });
    const database = createAdapter({
      batchResults: [
        { results: [{ version: 0 }], success: true },
        { success: true },
        { meta: { changes: 1 }, success: true },
      ],
      rules,
    });

    await expect(
      database.patch("todos", testId, { text: "after" })
    ).rejects.toThrow(
      "D1 did not report a change count for the write operation"
    );
  });

  it("requires D1 write batches to return one result per operation", async () => {
    const database = createAdapter({
      batchResults: [
        { results: [{ version: 0 }], success: true },
        { meta: { changes: 1 }, success: true },
      ],
      rules: defineRules({
        todos: {
          insert: () => true,
        },
      }),
    });

    await expect(database.insert("todos", { text: "after" })).rejects.toThrow(
      "D1 write batch returned an unexpected number of results"
    );
  });

  it("requires D1 change counts for direct inserts", async () => {
    const database = createAdapter({
      batchResults: [
        { results: [{ version: 0 }], success: true },
        { success: true },
        { meta: { changes: 1 }, success: true },
      ],
      rules: defineRules({
        todos: {
          insert: () => true,
        },
      }),
    });

    await expect(database.insert("todos", { text: "after" })).rejects.toThrow(
      "D1 did not report a change count for the write operation"
    );
  });

  it("accepts direct inserts with one reported D1 change", async () => {
    const database = createAdapter({
      batchResults: [
        { results: [{ version: 0 }], success: true },
        { meta: { changes: 1 }, success: true },
        { meta: { changes: 1 }, success: true },
      ],
      rules: defineRules({
        todos: {
          insert: () => true,
        },
      }),
    });

    await expect(database.insert("todos", { text: "after" })).resolves.toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
    );
  });

  it("gates direct inserts behind table-version bumps", async () => {
    const batchQueries: string[][] = [];
    const database = createAdapter({
      batchQueries,
      batchResults: [
        { results: [{ version: 0 }], success: true },
        { meta: { changes: 1 }, success: true },
        { meta: { changes: 1 }, success: true },
      ],
      rules: defineRules({
        todos: {
          insert: () => true,
        },
      }),
    });

    await database.insert("todos", { text: "after" });

    expect(batchQueries[0]?.[0]).toContain("SELECT version");
    expect(batchQueries[0]?.[1]).toContain("UPDATE _bf_table_versions");
    expect(batchQueries[0]?.[1]).toContain("NOT EXISTS");
    expect(batchQueries[0]?.[2]).toContain("WHERE changes() = 1");
  });

  it("reports missing direct write table versions as internal errors", async () => {
    const database = createAdapter({
      batchResults: [
        { results: [], success: true },
        { meta: { changes: 0 }, success: true },
        { meta: { changes: 0 }, success: true },
      ],
      rules: defineRules({
        todos: {
          insert: () => true,
        },
      }),
    });

    await expect(database.insert("todos", { text: "after" })).rejects.toThrow(
      'Missing internal table version row for "todos"'
    );
  });

  it("reports direct write guard misses as conflicts", async () => {
    const database = createAdapter({
      batchResults: [
        { results: [{ version: 0 }], success: true },
        { meta: { changes: 0 }, success: true },
        { meta: { changes: 0 }, success: true },
      ],
      rules: defineRules({
        todos: {
          update: () => true,
        },
      }),
    });

    await expect(
      database.patch("todos", testId, { text: "after" })
    ).rejects.toThrow("Document changed concurrently");
  });

  it("reports guarded direct write multi-change anomalies as internal errors", async () => {
    const database = createAdapter({
      batchResults: [
        { results: [{ version: 0 }], success: true },
        { meta: { changes: 2 }, success: true },
        { meta: { changes: 0 }, success: true },
      ],
      rules: defineRules({
        todos: {
          update: () => true,
        },
      }),
    });

    await expect(
      database.patch("todos", testId, { text: "after" })
    ).rejects.toThrow(
      'D1 write operation "bump-table-version" did not apply after its guard passed'
    );
  });

  it("reports unguarded direct write change anomalies as internal errors", async () => {
    const database = createAdapter({
      batchResults: [
        { results: [{ version: 0 }], success: true },
        { meta: { changes: 1 }, success: true },
        { meta: { changes: 2 }, success: true },
      ],
      rules: defineRules({
        todos: {
          update: () => true,
        },
      }),
    });

    await expect(
      database.patch("todos", testId, { text: "after" })
    ).rejects.toThrow(
      'D1 write operation "update" did not apply after its guard passed'
    );
  });

  it("coerces direct insert validation errors", async () => {
    await expect(createAdapter().insert("todos", {})).rejects.toThrow(
      "Invalid insert document"
    );
  });

  it("coerces direct patch validation errors", async () => {
    const database = createAdapter({
      rules: defineRules({
        todos: {
          update: () => true,
        },
      }),
    });

    await expect(
      database.patch("todos", testId, { text: 123 })
    ).rejects.toThrow("Invalid patch document");
  });

  it("coerces direct replace validation errors", async () => {
    const database = createAdapter({
      rules: defineRules({
        todos: {
          update: () => true,
        },
      }),
    });

    await expect(
      database.replace("todos", testId, { text: 123 })
    ).rejects.toThrow("Invalid replacement document");
  });
});
