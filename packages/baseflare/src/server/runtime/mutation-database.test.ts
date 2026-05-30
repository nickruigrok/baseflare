import { v } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { defineRules } from "../permissions/define-rules";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { InternalRuntimeError } from "./errors";
import {
  MutationDatabase,
  RetryableMutationConflictError,
  withMutationRetry,
} from "./mutation-database";
import type { D1Database, D1PreparedStatement, D1Result } from "./types";

class FakePreparedStatement implements D1PreparedStatement {
  private readonly allHandler: (query: string) => D1Result | Promise<D1Result>;
  private readonly query: string;

  constructor(
    query: string,
    allHandler: (query: string) => D1Result | Promise<D1Result>
  ) {
    this.allHandler = allHandler;
    this.query = query;
  }

  all<TRow = Record<string, unknown>>(): Promise<D1Result<TRow>> {
    return this.allHandler(this.query) as Promise<D1Result<TRow>>;
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

function createFakeDatabase(options: {
  batchResults: readonly D1Result[];
  tableVersion?: number;
}): D1Database {
  return {
    batch() {
      return Promise.resolve(options.batchResults);
    },
    prepare(query) {
      return new FakePreparedStatement(query, (statement) => {
        if (statement.includes("FROM _bf_table_versions")) {
          return {
            results:
              options.tableVersion === undefined
                ? []
                : [{ version: options.tableVersion }],
            success: true,
          };
        }

        return { results: [], success: true };
      });
    },
  };
}

function createMutationDatabase(database: D1Database): MutationDatabase {
  return new MutationDatabase({
    database,
    getContext: () => ({}) as never,
    rules,
    schema,
  });
}

describe("MutationDatabase", () => {
  it("requires D1 change counts for commit write operations", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [{ success: true }, { success: true }],
      })
    );

    await mutationDb.insert("todos", { text: "missing-meta" });

    await expect(mutationDb.commit()).rejects.toThrow(InternalRuntimeError);
    await expect(mutationDb.commit()).rejects.toThrow(
      'Mutation commit operation "insert" did not report a D1 change count'
    );
  });

  it("treats zero write changes as retryable conflicts", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          { meta: { changes: 1 }, success: true },
          { meta: { changes: 0 }, success: true },
        ],
      })
    );

    await mutationDb.insert("todos", { text: "zero-bump" });

    await expect(mutationDb.commit()).rejects.toThrow(
      RetryableMutationConflictError
    );
  });

  it("validates assertion results even when D1 reports select changes", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          { meta: { changes: 0 }, results: [], success: true },
          { meta: { changes: 1 }, success: true },
          { meta: { changes: 1 }, success: true },
        ],
        tableVersion: 0,
      })
    );

    await mutationDb.query("todos").count();
    await mutationDb.insert("todos", { text: "assertion-validation" });

    await expect(mutationDb.commit()).rejects.toThrow(InternalRuntimeError);
    await expect(mutationDb.commit()).rejects.toThrow(
      'Missing internal table version row for "todos"'
    );
  });

  it("requires at least one mutation retry attempt", async () => {
    await expect(withMutationRetry(async () => "ok", 0)).rejects.toThrow(
      "withMutationRetry requires at least one attempt"
    );
  });
});
