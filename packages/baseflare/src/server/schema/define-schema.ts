import { SchemaError } from "baseflare/values";

import {
  assertTableName,
  createIndexStatement,
  createTableStatement,
  type NormalizedSchemaTables,
  type Schema,
  type SchemaTables,
  type TableDefinition,
  type TableIndex,
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

function isScalarPartitionField(
  table: TableDefinition,
  fieldName: string
): boolean {
  const validator = table.fields[fieldName];
  const kind = validator?.definition.kind;
  return (
    kind === "boolean" ||
    kind === "enum" ||
    kind === "id" ||
    kind === "literal" ||
    kind === "number" ||
    kind === "string"
  );
}

function normalizeIndexes(
  tableName: string,
  table: TableDefinition
): readonly TableIndex[] {
  if (table.indexes.length === 0) {
    return [];
  }

  const explicitPartitionIndexes = table.indexes.filter(
    (index) => index.partition === true
  );

  if (explicitPartitionIndexes.length > 1) {
    throw new SchemaError(
      `Table "${tableName}" can define at most one partition index`
    );
  }

  if (table.indexes.length === 1) {
    const index = table.indexes[0];
    if (!index) {
      return [];
    }

    const partition = index.partition !== false;
    validatePartitionIndex(tableName, table, index, partition);
    return [{ ...index, partition }];
  }

  if (explicitPartitionIndexes.length === 0) {
    const allIndexesOptedOut = table.indexes.every(
      (index) => index.partition === false
    );
    if (!allIndexesOptedOut) {
      throw new SchemaError(
        `Table "${tableName}" has multiple indexes; mark one index with { partition: true } or explicitly opt indexes out with { partition: false }`
      );
    }
  }

  return table.indexes.map((index) => {
    const partition = index.partition === true;
    validatePartitionIndex(tableName, table, index, partition);
    return partition ? { ...index, partition } : { ...index, partition: false };
  });
}

function validatePartitionIndex(
  tableName: string,
  table: TableDefinition,
  index: TableIndex,
  partition: boolean
): void {
  if (!partition) {
    return;
  }

  for (const field of index.fields) {
    if (!isScalarPartitionField(table, field)) {
      throw new SchemaError(
        `Partition index "${index.name}" on table "${tableName}" must use only scalar fields`
      );
    }
  }
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
      indexes: normalizeIndexes(tableName, table),
    };
  }

  return createSchema(normalizedTables as NormalizedSchemaTables<TTables>);
}
