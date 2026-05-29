import {
  createIndexStatement,
  createTableVersionStatements,
  type Schema,
} from "../schema/types";

import {
  ensureSuccessfulD1Result,
  InternalRuntimeError,
  withDatabaseErrorHandling,
} from "./errors";
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

  await withDatabaseErrorHandling(
    "Failed to apply runtime schema",
    async () => {
      const results = await database.batch(
        statements.map((statement) => database.prepare(statement))
      );

      if (results.length !== statements.length) {
        throw new InternalRuntimeError(
          "Runtime schema application returned an unexpected number of D1 results"
        );
      }

      for (const [index, result] of results.entries()) {
        ensureSuccessfulD1Result(
          result,
          `Failed to apply schema statement "${statements[index] ?? "unknown"}"`
        );
      }
    }
  );
}
