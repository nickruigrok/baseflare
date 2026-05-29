import { v } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { defineRules } from "../permissions/define-rules";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { InternalRuntimeError } from "./errors";
import { MutationDatabase } from "./mutation-database";
import type { D1Database, D1PreparedStatement, D1Result } from "./types";

class FakePreparedStatement implements D1PreparedStatement {
  private readonly query: string;

  constructor(query: string) {
    this.query = query;
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
    return Promise.resolve(null);
  }

  run(): Promise<D1Result> {
    throw new Error(`Unexpected run for query: ${this.query}`);
  }
}

describe("MutationDatabase", () => {
  it("requires D1 change counts for commit write operations", async () => {
    const schema = defineSchema({
      todos: defineTable({
        text: v.string(),
      }),
    });
    const rules = defineRules({
      todos: {
        insert: () => true,
      },
    });
    const database: D1Database = {
      batch(statements) {
        return Promise.resolve(
          statements.map(() => ({ success: true }) satisfies D1Result)
        );
      },
      prepare(query) {
        return new FakePreparedStatement(query);
      },
    };
    const mutationDb = new MutationDatabase({
      database,
      getContext: () => ({}) as never,
      rules,
      schema,
    });

    await mutationDb.insert("todos", { text: "missing-meta" });

    await expect(mutationDb.commit()).rejects.toThrow(InternalRuntimeError);
    await expect(mutationDb.commit()).rejects.toThrow(
      'Mutation commit operation "insert" did not report a D1 change count'
    );
  });
});
