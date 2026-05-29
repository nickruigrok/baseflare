import {
  createIndexStatement,
  createTableVersionStatements,
  type Schema,
} from "../schema/types";

import { ensureSuccessfulD1Result, withDatabaseErrorHandling } from "./errors";
import type { D1Database } from "./types";

function createRuntimeTableStatement(tableName: string): string {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (_id TEXT PRIMARY KEY, _data TEXT NOT NULL, _rev INTEGER NOT NULL DEFAULT 0 CHECK(_rev >= 0))`;
}

export async function applyRuntimeSchema(
  database: D1Database,
  schema: Schema
): Promise<void> {
  const statements: string[] = [];
  const tableNames = Object.keys(schema.tables);

  for (const tableName of tableNames) {
    statements.push(createRuntimeTableStatement(tableName));
  }

  for (const [tableName, table] of Object.entries(schema.tables)) {
    for (const index of table.indexes) {
      statements.push(
        createIndexStatement(tableName, index).replace(
          "CREATE INDEX ",
          "CREATE INDEX IF NOT EXISTS "
        )
      );
    }
  }

  statements.push(...createTableVersionStatements(tableNames));

  for (const statement of statements) {
    await withDatabaseErrorHandling(
      `Failed to apply schema statement "${statement}"`,
      async () =>
        ensureSuccessfulD1Result(
          await database.prepare(statement).run(),
          `Failed to apply schema statement "${statement}"`
        )
    );
  }
}
