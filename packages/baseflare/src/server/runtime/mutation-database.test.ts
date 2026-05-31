import { v } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { defineRules } from "../permissions/define-rules";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { TABLE_VERSION_TABLE_NAME } from "../schema/types";
import type { StoredDocumentRow } from "./d1";
import { InternalRuntimeError } from "./errors";
import {
  MutationDatabase,
  RetryableMutationConflictError,
  withMutationRetry,
} from "./mutation-database";
import type {
  D1BindingValue,
  D1Database,
  D1PreparedStatement,
  D1Result,
} from "./types";

class FakePreparedStatement implements D1PreparedStatement {
  private readonly allHandler: (query: string) => D1Result | Promise<D1Result>;
  readonly params: D1BindingValue[] = [];
  readonly query: string;

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

  bind(...values: D1BindingValue[]): D1PreparedStatement {
    this.params.push(...values);
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

interface FakeReadResult {
  readonly rows?: readonly Record<string, unknown>[];
  readonly version?: number;
}

const schema = defineSchema({
  todos: defineTable({
    text: v.string(),
  }),
});

const rules = defineRules({
  todos: {
    delete: () => true,
    insert: () => true,
    read: () => true,
    update: () => true,
  },
});

function createFakeDatabase(options: {
  batchParams?: D1BindingValue[][][];
  batchQueries?: string[][];
  batchResults: readonly D1Result[];
  readResults?: readonly FakeReadResult[];
  tableVersion?: null | number;
}): D1Database {
  const readResults = [...(options.readResults ?? [])];
  return {
    batch(statements) {
      options.batchQueries?.push(
        statements.map((statement) =>
          statement instanceof FakePreparedStatement ? statement.query : ""
        )
      );
      options.batchParams?.push(
        statements.map((statement) =>
          statement instanceof FakePreparedStatement
            ? [...statement.params]
            : []
        )
      );

      const versionStatement = statements[0];
      const queryStatement = statements[1];
      if (
        statements.length === 2 &&
        versionStatement instanceof FakePreparedStatement &&
        queryStatement instanceof FakePreparedStatement &&
        versionStatement.query.includes(`FROM ${TABLE_VERSION_TABLE_NAME}`) &&
        queryStatement.query.includes("FROM todos")
      ) {
        const read = readResults.shift() ?? {
          version: options.tableVersion,
          rows: [],
        };
        return Promise.resolve([
          {
            results:
              read.version === undefined ? [] : [{ version: read.version }],
            success: true,
          },
          { results: read.rows ?? [], success: true },
        ]);
      }

      return Promise.resolve(options.batchResults);
    },
    prepare(query) {
      return new FakePreparedStatement(query, (statement) => {
        if (statement.includes("FROM _bf_table_versions")) {
          const version =
            options.tableVersion === undefined ? 0 : options.tableVersion;
          return {
            results: version === null ? [] : [{ version }],
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

function createStoredRow(index: number): StoredDocumentRow {
  return {
    _id: `019078e5-d29f-7000-8000-${index.toString(16).padStart(12, "0")}`,
    _data: JSON.stringify({ text: `todo-${index}` }),
    _rev: 0,
  };
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
      'Mutation commit operation "bump-table-version" did not report a D1 change count'
    );
  });

  it("treats guarded bump misses as retryable conflicts", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          { meta: { changes: 0 }, success: true },
          { meta: { changes: 0 }, success: true },
        ],
      })
    );

    await mutationDb.insert("todos", { text: "zero-bump" });

    await expect(mutationDb.commit()).rejects.toThrow(
      RetryableMutationConflictError
    );
  });

  it("requires internal table version rows before committing", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        tableVersion: null,
      })
    );

    await mutationDb.insert("todos", { text: "assertion-validation" });

    await expect(mutationDb.commit()).rejects.toThrow(InternalRuntimeError);
    await expect(mutationDb.commit()).rejects.toThrow(
      'Missing internal table version row for "todos"'
    );
  });

  it("reads query rows and table version in one D1 batch", async () => {
    const batchQueries: string[][] = [];
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchQueries,
        batchResults: [],
        readResults: [{ version: 0, rows: [createStoredRow(1)] }],
      })
    );

    const documents = await mutationDb.query("todos").collect();

    expect(documents.map((document) => document.text)).toEqual(["todo-1"]);
    expect(batchQueries[0]).toHaveLength(2);
    expect(batchQueries[0]?.[0]).toContain(`FROM ${TABLE_VERSION_TABLE_NAME}`);
    expect(batchQueries[0]?.[1]).toContain("FROM todos");
  });

  it("uses limited D1 chunks for limited mutation queries", async () => {
    const batchParams: D1BindingValue[][][] = [];
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchParams,
        batchResults: [],
        readResults: [{ version: 0, rows: [createStoredRow(1)] }],
      })
    );

    const documents = await mutationDb.query("todos").limit(1).collect();

    expect(documents.map((document) => document.text)).toEqual(["todo-1"]);
    expect(batchParams[0]?.[1]).toEqual([1]);
  });

  it("accounts for shadowed rows when sizing limited D1 chunks", async () => {
    const batchParams: D1BindingValue[][][] = [];
    const firstRow = createStoredRow(1);
    const secondRow = createStoredRow(2);
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchParams,
        batchResults: [],
        readResults: [
          { rows: [firstRow] },
          { rows: [secondRow] },
          { version: 0, rows: [firstRow, secondRow, createStoredRow(3)] },
        ],
      })
    );

    await mutationDb.patch("todos", firstRow._id, { text: "todo-1-updated" });
    await mutationDb.patch("todos", secondRow._id, {
      text: "todo-2-updated",
    });

    const documents = await mutationDb.query("todos").limit(1).collect();

    expect(documents).toHaveLength(1);
    expect(batchParams[2]?.[1]).toEqual([3]);
  });

  it("excludes shadowed rows from mutation scan budgets", async () => {
    const shadowedRow = {
      ...createStoredRow(1),
      _data: JSON.stringify({ text: "x".repeat(5_000_001) }),
    };
    const visibleRow = createStoredRow(2);
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        readResults: [
          { rows: [shadowedRow] },
          { version: 0, rows: [shadowedRow, visibleRow] },
        ],
      })
    );

    await mutationDb.delete("todos", shadowedRow._id);

    const documents = await mutationDb.query("todos").limit(1).collect();

    expect(documents.map((document) => document.text)).toEqual(["todo-2"]);
  });

  it("skips base reads for zero-limit mutation queries", async () => {
    const batchQueries: string[][] = [];
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchQueries,
        batchResults: [],
        readResults: [{ version: 0, rows: [createStoredRow(1)] }],
      })
    );

    await expect(mutationDb.query("todos").limit(0).collect()).resolves.toEqual(
      []
    );
    expect(batchQueries).toEqual([]);
  });

  it("retries when query chunks observe different table versions", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        readResults: [
          {
            version: 0,
            rows: Array.from({ length: 256 }, (_, index) =>
              createStoredRow(index + 1)
            ),
          },
          { version: 1, rows: [] },
        ],
      })
    );

    await expect(mutationDb.query("todos").collect()).rejects.toThrow(
      RetryableMutationConflictError
    );
  });

  it("records the batched table version for missing document reads", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          { meta: { changes: 0 }, success: true },
          { meta: { changes: 0 }, success: true },
        ],
        readResults: [{ version: 0, rows: [] }],
      })
    );

    await expect(
      mutationDb.get("todos", createStoredRow(1)._id)
    ).resolves.toBeNull();
    await mutationDb.insert("todos", { text: "after-missing-get" });

    await expect(mutationDb.commit()).rejects.toThrow(
      RetryableMutationConflictError
    );
  });

  it("does not require table versions for successful point reads", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        readResults: [{ rows: [createStoredRow(1)] }],
      })
    );

    await expect(
      mutationDb.get("todos", createStoredRow(1)._id)
    ).resolves.toMatchObject({
      text: "todo-1",
    });
  });

  it("does not commit assertions for read-only point reads", async () => {
    const batchQueries: string[][] = [];
    const row = createStoredRow(1);
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchQueries,
        batchResults: [{ results: [{ rowRev: row._rev }], success: true }],
        readResults: [{ version: 0, rows: [row] }],
      })
    );

    await mutationDb.get("todos", row._id);
    await mutationDb.commit();

    expect(batchQueries).toEqual([
      [
        "SELECT version FROM _bf_table_versions WHERE table_name = ? LIMIT 1",
        "SELECT _id, _data, _rev FROM todos WHERE _id = ? LIMIT 1",
      ],
    ]);
  });

  it("requires at least one mutation retry attempt", async () => {
    await expect(withMutationRetry(async () => "ok", 0)).rejects.toThrow(
      "withMutationRetry requires at least one attempt"
    );
  });

  it("keeps the sorted top documents when limiting overlay reads", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        tableVersion: 0,
      })
    );

    await mutationDb.insert("todos", { text: "c" });
    await mutationDb.insert("todos", { text: "a" });
    await mutationDb.insert("todos", { text: "b" });

    const documents = await mutationDb
      .query("todos")
      .order("text", "asc")
      .limit(2)
      .collect();

    expect(documents.map((document) => document.text)).toEqual(["a", "b"]);
  });

  it("keeps an empty overlay result for zero limits", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        tableVersion: 0,
      })
    );

    await mutationDb.insert("todos", { text: "a" });

    await expect(mutationDb.query("todos").limit(0).collect()).resolves.toEqual(
      []
    );
  });
});
