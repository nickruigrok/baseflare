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
  type CommitGuard,
  createGuardedTableVersionBumps,
  D1DatabaseAdapter,
  getNextRuntimeScanPosition,
  getRuntimeScanQueryOptions,
  type RuntimeScanPosition,
} from "./d1";
import { ValidationRuntimeError } from "./errors";
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
  readonly queryRows?: readonly Record<string, unknown>[];
}): D1Database {
  return {
    batch() {
      return Promise.resolve([]);
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
  readonly queryRows?: readonly Record<string, unknown>[];
  readonly rules?: ReturnType<typeof defineRules>;
}): D1DatabaseAdapter {
  return new D1DatabaseAdapter({
    database: createFakeDatabase({
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

  it("throws validation errors for duplicate unique results", async () => {
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

    const unique = database.query("todos").unique();
    await expect(unique).rejects.toBeInstanceOf(ValidationRuntimeError);
    await expect(unique).rejects.toThrow(
      'Expected exactly one document from "todos", received 2'
    );
  });

  it("builds guarded table-version bumps with all commit guard conditions", () => {
    const guard: CommitGuard = {
      insertedIds: new Map([["todos", new Set([nextTestId])]]),
      rowRevisions: new Map([["labels", new Map([[testId, 7]])]]),
      tableVersions: new Map([
        ["labels", 2],
        ["todos", 3],
      ]),
    };

    const bump = createGuardedTableVersionBumps(["labels", "todos"], guard);

    expect(bump.sql).toContain("table_name IN (?, ?)");
    expect(bump.sql).toContain(
      "EXISTS (SELECT 1 FROM _bf_table_versions WHERE table_name = ? AND version = ?)"
    );
    expect(bump.sql).toContain(
      "EXISTS (SELECT 1 FROM labels WHERE _id = ? AND _rev = ?)"
    );
    expect(bump.sql).toContain(
      "NOT EXISTS (SELECT 1 FROM todos WHERE _id = ?)"
    );
    expect(bump.params).toEqual([
      "labels",
      "todos",
      "labels",
      2,
      "todos",
      3,
      testId,
      7,
      nextTestId,
    ]);
  });

  it("rejects empty guarded table-version bumps", () => {
    const guard: CommitGuard = {
      insertedIds: new Map(),
      rowRevisions: new Map(),
      tableVersions: new Map(),
    };

    expect(() => createGuardedTableVersionBumps([], guard)).toThrow(
      "Guarded table-version bump requires at least one table"
    );
  });
});
