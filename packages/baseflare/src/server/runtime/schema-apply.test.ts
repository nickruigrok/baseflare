import { v } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { applyRuntimeSchema } from "./schema-apply";
import type { D1Database, D1PreparedStatement, D1Result } from "./types";

class FakePreparedStatement implements D1PreparedStatement {
  readonly params: unknown[] = [];
  readonly query: string;

  constructor(query: string) {
    this.query = query;
  }

  all<TRow = Record<string, unknown>>(): Promise<D1Result<TRow>> {
    return Promise.resolve({ results: [], success: true });
  }

  bind(...values: unknown[]): D1PreparedStatement {
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
    throw new Error("Schema apply should use batch instead of run");
  }
}

describe("runtime schema apply", () => {
  it("validates runtime schema table identifiers before preparing DDL", async () => {
    const schema = {
      tables: {
        "bad table": defineTable({ text: v.string() }),
      },
    } as unknown as ReturnType<typeof defineSchema>;
    const database: D1Database = {
      prepare() {
        throw new Error("Schema apply should validate before preparing");
      },
      batch() {
        return Promise.resolve([]);
      },
    };

    await expect(applyRuntimeSchema(database, schema)).rejects.toThrow(
      /must start with a letter/
    );
  });

  it("applies all schema statements in one D1 batch", async () => {
    const schema = defineSchema({
      todos: defineTable({
        ownerToken: v.string(),
        text: v.string(),
      }).index("by_owner", ["ownerToken"]),
    });
    const preparedStatements: FakePreparedStatement[] = [];
    const batchCalls: FakePreparedStatement[][] = [];
    const database: D1Database = {
      prepare(query) {
        const statement = new FakePreparedStatement(query);
        preparedStatements.push(statement);
        return statement;
      },
      batch(statements) {
        batchCalls.push(statements as FakePreparedStatement[]);
        return Promise.resolve(
          statements.map(() => ({ success: true }) satisfies D1Result)
        );
      },
    };

    await applyRuntimeSchema(database, schema);

    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0]).toEqual(preparedStatements);
    expect(preparedStatements.map((statement) => statement.query)).toEqual([
      "CREATE TABLE IF NOT EXISTS todos (_id TEXT PRIMARY KEY, _data TEXT NOT NULL, _rev INTEGER NOT NULL DEFAULT 0 CHECK(_rev >= 0))",
      "CREATE INDEX IF NOT EXISTS todos_by_owner ON todos (json_extract(_data, '$.ownerToken'))",
      "CREATE TABLE IF NOT EXISTS _bf_table_versions (table_name TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0))",
      "INSERT OR IGNORE INTO _bf_table_versions (table_name, version) VALUES (?, 0)",
      "CREATE TABLE IF NOT EXISTS _bf_partition_versions (table_name TEXT NOT NULL, partition_key TEXT NOT NULL, partition_value TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0), PRIMARY KEY (table_name, partition_key, partition_value))",
      "CREATE TABLE IF NOT EXISTS _bf_realtime_outbox (sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, created_at INTEGER NOT NULL, tables TEXT NOT NULL, partitions TEXT NOT NULL)",
      "CREATE INDEX IF NOT EXISTS _bf_realtime_outbox_created_at ON _bf_realtime_outbox (created_at)",
    ]);
    expect(preparedStatements.map((statement) => statement.params)).toEqual([
      [],
      [],
      [],
      ["todos"],
      [],
      [],
      [],
    ]);
  });
});
