import { v } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { defineRules } from "../permissions/define-rules";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import type { Schema } from "../schema/types";
import {
  assertKnownTable,
  assertWithinScanBudget,
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
      "Query exceeded the internal scan budget"
    );
    expect(() => assertWithinScanBudget(1, 5_000_001)).toThrow(
      "Query exceeded the internal scan budget"
    );
  });

  it("requires D1 change counts for direct write operations", async () => {
    const schema = defineSchema({
      todos: defineTable({
        text: v.string(),
      }),
    });
    const rules = defineRules({
      todos: {
        update: () => true,
      },
    });
    const database = new D1DatabaseAdapter({
      database: createFakeDatabase([
        { success: true },
        { meta: { changes: 1 }, success: true },
      ]),
      getContext: () => ({}),
      rules,
      schema,
    });

    await expect(
      database.patch("todos", testId, { text: "after" })
    ).rejects.toThrow(
      "D1 did not report a change count for the write operation"
    );
  });
});
