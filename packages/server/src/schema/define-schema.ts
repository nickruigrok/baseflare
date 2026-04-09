import {
  assertTableName,
  createIndexStatement,
  createTableStatement,
  type Schema,
  type TableDefinition,
} from "./types";

function createSchema(tables: Record<string, TableDefinition>): Schema {
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

export function defineSchema(tables: Record<string, TableDefinition>): Schema {
  if (Object.keys(tables).length === 0) {
    throw new Error("Schemas must define at least one table");
  }

  const normalizedTables: Record<string, TableDefinition> = {};

  for (const [tableName, table] of Object.entries(tables)) {
    assertTableName(tableName);
    normalizedTables[tableName] = {
      fields: { ...table.fields },
      indexes: [...table.indexes],
    };
  }

  return createSchema(normalizedTables);
}
