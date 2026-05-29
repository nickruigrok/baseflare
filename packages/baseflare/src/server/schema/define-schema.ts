import { SchemaError } from "baseflare/values";

import {
  assertTableName,
  createIndexStatement,
  createTableStatement,
  type NormalizedSchemaTables,
  type Schema,
  type SchemaTables,
  type TableDefinition,
} from "./types";

function createSchema<TTables extends SchemaTables>(
  tables: TTables
): Schema<TTables> {
  return {
    tables,
    toCreateStatements(): string[] {
      const statements: string[] = [];

      for (const tableName of Object.keys(tables)) {
        statements.push(createTableStatement(tableName));
      }

      for (const [tableName, table] of Object.entries(tables)) {
        for (const index of table.indexes) {
          statements.push(createIndexStatement(tableName, index));
        }
      }

      return statements;
    },
  };
}

export function defineSchema<TTables extends SchemaTables>(
  tables: TTables
): Schema<NormalizedSchemaTables<TTables>> {
  if (Object.keys(tables).length === 0) {
    throw new SchemaError("Schemas must define at least one table");
  }

  const normalizedTables: Record<string, TableDefinition> = {};

  for (const [tableName, table] of Object.entries(tables)) {
    assertTableName(tableName);
    normalizedTables[tableName] = {
      fields: { ...table.fields },
      indexes: [...table.indexes],
    };
  }

  return createSchema(normalizedTables as NormalizedSchemaTables<TTables>);
}
