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
} from "./d1";
import type { D1Database, D1PreparedStatement, D1Result } from "./types";

class FakePreparedStatement implements D1PreparedStatement {
  private readonly firstResult: Record<string, unknown> | null;

  constructor(firstResult: Record<string, unknown> | null) {
    this.firstResult = firstResult;
  }

  all<TRow = Record<string, unknown>>(): Promise<D1Result<TRow>> {
    return Promise.resolve({ results: [], success: true });
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

function createFakeDatabase(batchResults: readonly D1Result[]): D1Database {
  return {
    batch() {
      return Promise.resolve(batchResults);
    },
    prepare() {
      return new FakePreparedStatement({
        _id: testId,
        _data: JSON.stringify({ text: "before" }),
        _rev: 0,
      });
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
  readonly batchResults?: readonly D1Result[];
  readonly rules?: ReturnType<typeof defineRules>;
}): D1DatabaseAdapter {
  return new D1DatabaseAdapter({
    database: createFakeDatabase(options?.batchResults ?? []),
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
      "Query exceeded the internal scan budget; add a more selective filter or limit"
    );
    expect(() => assertWithinScanBudget(1, 5_000_001)).toThrow(
      "Query exceeded the internal scan budget; add a more selective filter or limit"
    );
  });

  it("validates runtime select table identifiers", () => {
    expect(() =>
      buildRuntimeSelectQuery("bad table", createBaseQueryState(), {
        limit: 1,
      })
    ).toThrow(/must start with a letter/);
  });

  it("requires D1 change counts for direct write operations", async () => {
    const rules = defineRules({
      todos: {
        update: () => true,
      },
    });
    const database = createAdapter({
      batchResults: [
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

  it("requires D1 change counts for direct inserts", async () => {
    const database = createAdapter({
      batchResults: [
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
