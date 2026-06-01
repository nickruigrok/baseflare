import { v } from "baseflare/values";
import { describe, expect, it, vi } from "vitest";

import type { CursorPayload } from "../db/cursor";
import type { QueryState } from "../db/query-builder";
import { createBaseQueryState } from "../db/query-builder";
import { defineRules } from "../permissions/define-rules";
import type { Rules } from "../permissions/types";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { TABLE_VERSION_TABLE_NAME } from "../schema/types";
import type { StoredDocumentRow } from "./d1";
import { InternalRuntimeError } from "./errors";
import {
  createMutationDatabaseSession,
  MutationDatabase,
  RetryableMutationConflictError,
  withMutationRetry,
} from "./mutation-database";
import type {
  D1BindingValue,
  D1Database,
  D1DatabaseSession,
  D1PreparedStatement,
  D1Result,
} from "./types";

class FakePreparedStatement implements D1PreparedStatement {
  private readonly allHandler: (
    query: string,
    params: readonly D1BindingValue[]
  ) => D1Result | Promise<D1Result>;
  readonly params: D1BindingValue[] = [];
  readonly query: string;

  constructor(
    query: string,
    allHandler: (
      query: string,
      params: readonly D1BindingValue[]
    ) => D1Result | Promise<D1Result>
  ) {
    this.allHandler = allHandler;
    this.query = query;
  }

  all<TRow = Record<string, unknown>>(): Promise<D1Result<TRow>> {
    return this.allHandler(this.query, this.params) as Promise<D1Result<TRow>>;
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
  labels: defineTable({
    text: v.string(),
  }),
  todos: defineTable({
    text: v.string(),
  }),
});

const rules = defineRules({
  labels: {
    delete: () => true,
    insert: () => true,
    read: () => true,
    update: () => true,
  },
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
  queryLog?: string[];
  readResults?: readonly FakeReadResult[];
  tableVersion?: null | number;
  tableVersionReads?: readonly (null | number)[];
  tableVersions?: Readonly<Record<string, number>>;
}): D1Database {
  const readResults = [...(options.readResults ?? [])];
  const tableVersionReads = [...(options.tableVersionReads ?? [])];
  const readTableVersion = (): null | number => {
    if (tableVersionReads.length > 0) {
      return tableVersionReads.shift() ?? null;
    }

    return options.tableVersion === undefined ? 0 : options.tableVersion;
  };

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
      options.queryLog?.push(query);
      return new FakePreparedStatement(query, (statement, params) => {
        if (statement.includes("FROM _bf_table_versions")) {
          if (statement.includes(" IN ")) {
            const tableVersions = options.tableVersions ?? {};
            const versionOverride = tableVersionReads.shift();
            return {
              results: params
                .map((tableName) => {
                  if (typeof tableName !== "string") {
                    return undefined;
                  }

                  const version =
                    versionOverride === undefined
                      ? (tableVersions[tableName] ?? readTableVersion())
                      : versionOverride;
                  return version === null
                    ? undefined
                    : { table_name: tableName, version };
                })
                .filter((row) => row !== undefined),
              success: true,
            };
          }

          const version = readTableVersion();
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

function createMutationDatabase(
  database: D1Database,
  options: {
    readonly missingRules?: boolean;
    readonly rules?: Rules;
  } = {}
): MutationDatabase {
  return new MutationDatabase({
    database,
    getContext: () => ({}) as never,
    rules: options.missingRules ? undefined : (options.rules ?? rules),
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

function tableVersionResult(
  tableVersions: Readonly<Record<string, number>>
): D1Result {
  return {
    results: Object.entries(tableVersions).map(([table_name, version]) => ({
      table_name,
      version,
    })),
    success: true,
  };
}

describe("MutationDatabase", () => {
  it("fails closed when a mutated table has no committed writes", () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({ batchResults: [] })
    );
    const buildCommitOperations = (
      mutationDb as unknown as {
        buildCommitOperations(tableNames: readonly string[]): unknown[];
      }
    ).buildCommitOperations.bind(mutationDb);
    const appendWriteStatements = (
      mutationDb as unknown as {
        appendWriteStatementsForTable(
          operations: unknown[],
          tableName: string,
          expectedPreviousChanges: number
        ): number;
      }
    ).appendWriteStatementsForTable.bind(mutationDb);

    expect(() => buildCommitOperations(["todos"])).toThrow(
      'Mutated table "todos" has no committed writes'
    );
    expect(() => appendWriteStatements([], "todos", 1)).toThrow(
      'Mutated table "todos" has no committed writes'
    );
  });

  it("creates primary sessions without re-wrapping existing sessions", () => {
    let rootSessionConstraint: string | undefined;
    let nestedSessionCalled = false;
    const session: D1DatabaseSession & {
      withSession(constraint?: string): D1DatabaseSession;
    } = {
      batch() {
        return Promise.resolve([]);
      },
      getBookmark() {
        return "bookmark";
      },
      prepare(query) {
        return new FakePreparedStatement(query, () => ({
          results: [],
          success: true,
        }));
      },
      withSession() {
        nestedSessionCalled = true;
        return this;
      },
    };
    const database: D1Database = {
      batch() {
        return Promise.resolve([]);
      },
      prepare(query) {
        return new FakePreparedStatement(query, () => ({
          results: [],
          success: true,
        }));
      },
      withSession(constraint) {
        rootSessionConstraint = constraint;
        return session;
      },
    };

    const rootSession = createMutationDatabaseSession(database);
    expect(rootSession).toBe(session);
    expect(rootSessionConstraint).toBe("first-primary");
    expect(rootSession).toMatchObject({
      batch: expect.any(Function),
      getBookmark: expect.any(Function),
      prepare: expect.any(Function),
    });

    const existingSession = createMutationDatabaseSession(session);
    expect(existingSession).toBe(session);
    expect(existingSession).toMatchObject({
      batch: expect.any(Function),
      getBookmark: expect.any(Function),
      prepare: expect.any(Function),
    });
    expect(nestedSessionCalled).toBe(false);
  });

  it("requires D1 Sessions for mutation consistency", () => {
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const database: D1Database = {
      batch() {
        return Promise.resolve([]);
      },
      prepare(query) {
        return new FakePreparedStatement(query, () => ({
          results: [],
          success: true,
        }));
      },
    };

    try {
      expect(() => createMutationDatabaseSession(database)).toThrow(
        "Baseflare runtime misconfiguration: APP_DB does not support D1 Sessions required for consistent mutations."
      );
      expect(consoleError).toHaveBeenCalledWith(
        "baseflare-runtime",
        expect.objectContaining({
          component: "baseflare-runtime",
          event: "runtime.d1_session_required",
        })
      );
    } finally {
      consoleError.mockRestore();
    }
  });

  it("requires D1 change counts for commit write operations", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          tableVersionResult({ todos: 0 }),
          { success: true },
          { success: true },
        ],
      })
    );

    await mutationDb.insert("todos", { text: "missing-meta" });

    await expect(mutationDb.commit()).rejects.toThrow(InternalRuntimeError);
    await expect(mutationDb.commit()).rejects.toThrow(
      'Mutation commit operation "bump-table-versions" did not report a D1 change count'
    );
  });

  it("includes the table name in duplicate mutation unique errors", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        tableVersion: 0,
      })
    );

    await mutationDb.insert("todos", { text: "first" });
    await mutationDb.insert("todos", { text: "second" });

    await expect(mutationDb.query("todos").unique()).rejects.toThrow(
      'Expected exactly one document from "todos", received 2'
    );
  });

  it("treats guarded bump misses as retryable conflicts", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          tableVersionResult({ todos: 0 }),
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

  it("treats under-applied multi-table bumps as retryable conflicts", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          tableVersionResult({ labels: 0, todos: 0 }),
          { meta: { changes: 1 }, success: true },
          { meta: { changes: 0 }, success: true },
          { meta: { changes: 0 }, success: true },
        ],
      })
    );

    await mutationDb.insert("todos", { text: "todo" });
    await mutationDb.insert("labels", { text: "label" });

    await expect(mutationDb.commit()).rejects.toThrow(
      RetryableMutationConflictError
    );
  });

  it("reports over-applied multi-table bumps as internal errors", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          tableVersionResult({ labels: 0, todos: 0 }),
          { meta: { changes: 3 }, success: true },
          { meta: { changes: 0 }, success: true },
          { meta: { changes: 0 }, success: true },
        ],
      })
    );

    await mutationDb.insert("todos", { text: "todo" });
    await mutationDb.insert("labels", { text: "label" });

    await expect(mutationDb.commit()).rejects.toThrow(
      'Mutation commit operation "bump-table-versions" applied 3 rows but expected 2'
    );
  });

  it("requires internal table version rows before committing", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          { results: [], success: true },
          { meta: { changes: 0 }, success: true },
          { meta: { changes: 0 }, success: true },
        ],
      })
    );

    await mutationDb.insert("todos", { text: "assertion-validation" });

    await expect(mutationDb.commit()).rejects.toThrow(
      'Missing internal table version row for "todos"; run applyRuntimeSchema before handling runtime traffic'
    );
  });

  it("prevents document writes when a multi-table commit gate fails", async () => {
    const batchParams: D1BindingValue[][][] = [];
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchParams,
        batchResults: [
          tableVersionResult({ labels: 0, todos: 0 }),
          { meta: { changes: 0 }, success: true },
          { meta: { changes: 0 }, success: true },
          { meta: { changes: 1 }, success: true },
        ],
      })
    );

    await mutationDb.insert("todos", { text: "todo" });
    await mutationDb.insert("labels", { text: "label" });

    await expect(mutationDb.commit()).rejects.toThrow(
      RetryableMutationConflictError
    );
    expect(batchParams[0]?.[2]?.at(-1)).toBe(2);
    expect(batchParams[0]?.[3]?.at(-1)).toBe(1);
  });

  it("commits successful multi-table writes behind one table-version gate", async () => {
    const batchQueries: string[][] = [];
    const batchParams: D1BindingValue[][][] = [];
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchParams,
        batchQueries,
        batchResults: [
          tableVersionResult({ labels: 0, todos: 0 }),
          { meta: { changes: 2 }, success: true },
          { meta: { changes: 1 }, success: true },
          { meta: { changes: 1 }, success: true },
        ],
      })
    );

    await mutationDb.insert("todos", { text: "todo" });
    await mutationDb.insert("labels", { text: "label" });
    await mutationDb.commit();

    expect(batchQueries[0]).toHaveLength(4);
    expect(batchQueries[0]?.[0]).toContain(
      "SELECT table_name, version FROM _bf_table_versions"
    );
    expect(batchQueries[0]?.[0]).toContain("table_name IN (?, ?)");
    expect(batchParams[0]?.[0]?.slice(0, 2)).toEqual(["labels", "todos"]);
    expect(batchQueries[0]?.[1]).toContain("UPDATE _bf_table_versions");
    expect(batchParams[0]?.[1]?.slice(0, 2)).toEqual(["labels", "todos"]);
    expect(batchQueries[0]?.[2]).toContain("WHERE changes() = ?");
    expect(batchParams[0]?.[2]?.at(-1)).toBe(2);
    expect(batchQueries[0]?.[3]).toContain("WHERE changes() = ?");
    expect(batchParams[0]?.[3]?.at(-1)).toBe(1);
  });

  it("builds document writes behind the multi-table version gate", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
      })
    );
    const buildCommitOperations = (
      mutationDb as unknown as {
        buildCommitOperations(tableNames: readonly string[]): Array<{
          readonly params: readonly unknown[];
          readonly sql: string;
          readonly type: string;
        }>;
      }
    ).buildCommitOperations.bind(mutationDb);

    await mutationDb.insert("todos", { text: "todo" });
    await mutationDb.insert("labels", { text: "label" });

    const operations = buildCommitOperations(["todos", "labels"]);

    expect(operations.map((operation) => operation.type)).toEqual([
      "assert-table-versions",
      "bump-table-versions",
      "insert",
      "insert",
    ]);
    expect(operations[2]?.sql).toContain("WHERE changes() = ?");
    expect(operations[2]?.params.at(-1)).toBe(2);
    expect(operations[3]?.sql).toContain("WHERE changes() = ?");
    expect(operations[3]?.params.at(-1)).toBe(1);
  });

  it("checks stale read versions when a table is also mutated", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          tableVersionResult({ todos: 1 }),
          { meta: { changes: 0 }, success: true },
          { meta: { changes: 0 }, success: true },
        ],
        readResults: [{ version: 0, rows: [] }],
      })
    );

    await mutationDb.query("todos").collect();
    await mutationDb.insert("todos", { text: "stale" });

    await expect(mutationDb.commit()).rejects.toThrow(
      RetryableMutationConflictError
    );
  });

  it("batches table version assertions with commit operations", async () => {
    const queryLog: string[] = [];
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          tableVersionResult({ todos: 0 }),
          { meta: { changes: 1 }, success: true },
          { meta: { changes: 1 }, success: true },
        ],
        queryLog,
        readResults: [{ version: 0, rows: [] }],
      })
    );

    await mutationDb.query("todos").collect();
    await mutationDb.insert("todos", { text: "one-query" });
    await mutationDb.commit();

    expect(
      queryLog.filter((query) =>
        query.includes("SELECT table_name, version FROM _bf_table_versions")
      )
    ).toEqual([
      "SELECT table_name, version FROM _bf_table_versions WHERE table_name IN (?)",
    ]);
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

  it("advances multi-chunk mutation scans with keyset predicates", async () => {
    const batchQueries: string[][] = [];
    const rows = Array.from({ length: 256 }, (_, index) =>
      createStoredRow(index + 1)
    );
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchQueries,
        batchResults: [],
        readResults: [
          { version: 0, rows },
          { version: 0, rows: [] },
        ],
      })
    );

    await expect(mutationDb.query("todos").collect()).resolves.toHaveLength(
      256
    );

    expect(batchQueries[1]?.[1]).toContain("_id > ?");
    expect(batchQueries[1]?.[1]).not.toContain("OFFSET");
  });

  it("preserves the base cursor when mutation scans fall back to offsets", async () => {
    const batchQueries: string[][] = [];
    const batchParams: D1BindingValue[][][] = [];
    const rows = Array.from({ length: 256 }, (_, index) => ({
      ...createStoredRow(index + 1),
      _data: JSON.stringify({ text: { nested: true } }),
    }));
    const denyRules = defineRules({
      labels: {
        delete: () => false,
        insert: () => false,
        read: () => false,
        update: () => false,
      },
      todos: {
        delete: () => false,
        insert: () => false,
        read: () => false,
        update: () => false,
      },
    });
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchParams,
        batchQueries,
        batchResults: [],
        readResults: [
          { version: 0, rows },
          { version: 0, rows: [] },
        ],
      }),
      { rules: denyRules }
    );
    const collectQuery = (
      mutationDb as unknown as {
        collectQuery(
          tableName: string,
          state: QueryState,
          cursor: CursorPayload
        ): Promise<unknown[]>;
      }
    ).collectQuery.bind(mutationDb);

    await expect(
      collectQuery(
        "todos",
        {
          ...createBaseQueryState(),
          order: { field: "text", direction: "asc" },
        },
        {
          id: "019078e5-d29f-7000-8000-000000000000",
          orderDirection: "asc",
          orderField: "text",
          v: "alpha",
        }
      )
    ).resolves.toEqual([]);

    expect(batchQueries[1]?.[1]).toContain(
      "(json_extract(_data, '$.text') > ? OR (json_extract(_data, '$.text') = ? AND _id > ?))"
    );
    expect(batchQueries[1]?.[1]).toContain("OFFSET ?");
    expect(batchParams[1]?.[1]?.slice(-2)).toEqual([256, 256]);
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

  it("does not count pending inserts as shadowed base rows for chunk sizing", async () => {
    const batchParams: D1BindingValue[][][] = [];
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchParams,
        batchResults: [],
        readResults: [{ version: 0, rows: [createStoredRow(1)] }],
      })
    );

    for (let index = 0; index < 5; index += 1) {
      await mutationDb.insert("todos", { text: `pending-${index}` });
    }

    const documents = await mutationDb.query("todos").limit(1).collect();

    expect(documents).toHaveLength(1);
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

  it("records table versions without base rows for zero-limit mutation queries", async () => {
    const queryLog: string[] = [];
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        queryLog,
        tableVersion: 0,
      })
    );

    await expect(mutationDb.query("todos").limit(0).collect()).resolves.toEqual(
      []
    );
    expect(queryLog).toEqual([
      "SELECT version FROM _bf_table_versions WHERE table_name = ? LIMIT 1",
      "SELECT _id, _data, _rev FROM todos LIMIT 0",
    ]);
  });

  it("retries stale writes after zero-limit mutation queries", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [
          tableVersionResult({ todos: 1 }),
          { meta: { changes: 0 }, success: true },
          { meta: { changes: 0 }, success: true },
        ],
        readResults: [{ version: 0, rows: [] }],
      })
    );

    await expect(mutationDb.query("todos").limit(0).collect()).resolves.toEqual(
      []
    );
    await mutationDb.insert("todos", { text: "after-zero-limit" });

    await expect(mutationDb.commit()).rejects.toThrow(
      RetryableMutationConflictError
    );
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
          tableVersionResult({ todos: 1 }),
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

  it("logs remaining attempts when scheduling mutation retries", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    let attempts = 0;

    try {
      await expect(
        withMutationRetry(
          () => {
            attempts += 1;
            if (attempts === 1) {
              throw new RetryableMutationConflictError();
            }

            return Promise.resolve("ok");
          },
          2,
          "todos:create"
        )
      ).resolves.toBe("ok");

      expect(warn).toHaveBeenCalledWith(
        "baseflare-runtime",
        expect.objectContaining({
          attempt: 1,
          event: "mutation.retry_scheduled",
          functionName: "todos:create",
          maxAttempts: 2,
          remainingAttempts: 1,
        })
      );
    } finally {
      warn.mockRestore();
    }
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
    const queryLog: string[] = [];
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        queryLog,
        tableVersion: 0,
      })
    );

    await mutationDb.insert("todos", { text: "a" });

    await expect(mutationDb.query("todos").limit(0).collect()).resolves.toEqual(
      []
    );
    expect(queryLog).toEqual([
      "SELECT version FROM _bf_table_versions WHERE table_name = ? LIMIT 1",
      "SELECT _id, _data, _rev FROM todos LIMIT 0",
    ]);
  });

  it("applies scan budgets to pending overlay documents", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        tableVersion: 0,
      })
    );

    await mutationDb.insert("todos", { text: "x".repeat(5_000_001) });

    await expect(mutationDb.query("todos").collect()).rejects.toThrow(
      "Query exceeded the internal scan budget; add a more selective filter"
    );
  });

  it("requires serialized data for pending overlay scan budgeting", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        tableVersion: 0,
      })
    );
    const id = await mutationDb.insert("todos", { text: "pending" });
    const pendingWrites = (
      mutationDb as unknown as {
        pendingWrites: Map<
          string,
          Map<
            string,
            {
              document?: Record<string, unknown>;
              serializedData?: string;
              type: string;
            }
          >
        >;
      }
    ).pendingWrites;
    const writes = pendingWrites.get("todos");
    const write = writes?.get(id);

    expect(write).toBeDefined();
    if (!(writes && write)) {
      throw new Error("Expected pending write");
    }
    writes.set(id, { document: write.document, type: write.type });

    await expect(mutationDb.query("todos").collect()).rejects.toThrow(
      "Pending mutation write is missing serialized data"
    );
  });

  it("shares one scan budget across base and overlay documents", async () => {
    const baseRow = {
      ...createStoredRow(1),
      _data: JSON.stringify({ text: "x".repeat(4_000_000) }),
    };
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        readResults: [{ version: 0, rows: [baseRow] }],
      })
    );

    await mutationDb.insert("todos", { text: "x".repeat(1_000_001) });

    await expect(mutationDb.query("todos").collect()).rejects.toThrow(
      "Query exceeded the internal scan budget; add a more selective filter"
    );
  });

  it("uses count-specific scan budget diagnostics", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        tableVersion: 0,
      })
    );

    await mutationDb.insert("todos", { text: "x".repeat(5_000_001) });

    await expect(mutationDb.query("todos").count()).rejects.toThrow(
      "Count exceeded the internal scan budget; add a more selective filter before count()"
    );
  });

  it("throws visibly when mutation read rules are missing", async () => {
    const mutationDb = createMutationDatabase(
      createFakeDatabase({
        batchResults: [],
        readResults: [{ version: 0, rows: [createStoredRow(1)] }],
      }),
      { missingRules: true }
    );

    await expect(
      mutationDb.get("todos", createStoredRow(1)._id)
    ).rejects.toThrow("Read rules are not configured");
    await expect(mutationDb.query("todos").collect()).rejects.toThrow(
      "Read rules are not configured"
    );
  });
});
