import type { AnyValidator } from "@baseflare/values";

const IDENTIFIER_PATTERN = /^[A-Za-z][A-Za-z0-9_]*$/;

export interface TableIndex {
  readonly fields: readonly string[];
  readonly name: string;
}

export interface TableDefinition {
  readonly fields: Record<string, AnyValidator>;
  readonly indexes: readonly TableIndex[];
}

export interface TableDefBuilder extends TableDefinition {
  index(name: string, fields: readonly string[]): TableDefBuilder;
}

export interface Schema {
  readonly tables: Record<string, TableDefinition>;
  toCreateStatements(): string[];
}

export interface DiffedIndex {
  readonly index: TableIndex;
  readonly tableName: string;
}

export interface SchemaDiff {
  readonly addedIndexes: readonly DiffedIndex[];
  readonly addedTables: Record<string, TableDefinition>;
  readonly hasChanges: boolean;
  readonly removedIndexes: readonly DiffedIndex[];
  readonly removedTables: string[];
  toStatements(): string[];
}

export function assertIdentifier(name: string, label: string): void {
  if (!IDENTIFIER_PATTERN.test(name)) {
    throw new Error(
      `${label} "${name}" must start with a letter and contain only letters, numbers, and underscores`
    );
  }
}

export function assertTableName(name: string): void {
  if (name.startsWith("_")) {
    throw new Error(`Table name "${name}" cannot start with "_"`);
  }

  assertIdentifier(name, "Table name");
}

export function assertFieldName(name: string): void {
  if (name.startsWith("_")) {
    throw new Error(`Field name "${name}" cannot start with "_"`);
  }

  assertIdentifier(name, "Field name");
}

export function createTableStatement(tableName: string): string {
  return `CREATE TABLE ${tableName} (_id TEXT PRIMARY KEY, _data TEXT NOT NULL)`;
}

export function createIndexStatement(
  tableName: string,
  index: TableIndex
): string {
  const expressions = index.fields
    .map((field) => `json_extract(_data, '$.${field}')`)
    .join(", ");

  return `CREATE INDEX ${tableName}_${index.name} ON ${tableName} (${expressions})`;
}
