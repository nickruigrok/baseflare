import {
  type Id,
  type ObjectOutput,
  SchemaError,
  type ValidatorShape,
} from "baseflare/values";

const IDENTIFIER_PATTERN = /^[A-Za-z][A-Za-z0-9_]*$/;
const FILTER_LOGIC_FIELD_NAMES = new Set(["AND", "OR", "NOT"]);

export interface TableIndex {
  readonly fields: readonly string[];
  readonly name: string;
}

export interface TableDefinition<
  TFields extends ValidatorShape = ValidatorShape,
> {
  readonly fields: TFields;
  readonly indexes: readonly TableIndex[];
}

export interface TableDefBuilder<
  TFields extends ValidatorShape = ValidatorShape,
> extends TableDefinition<TFields> {
  index(name: string, fields: readonly string[]): TableDefBuilder<TFields>;
}

export type SchemaTables = Record<string, TableDefinition>;

export interface Schema<TTables extends SchemaTables = SchemaTables> {
  readonly tables: TTables;
  toCreateStatements(): string[];
}

export type NormalizedSchemaTables<TTables extends SchemaTables> = {
  readonly [TName in keyof TTables]: TTables[TName] extends TableDefinition<
    infer TFields
  >
    ? TableDefinition<TFields>
    : never;
};

/**
 * The runtime shape of a stored document for a table: the developer fields plus
 * the framework-managed `_id` (branded by table) and `_createdAt` (ms epoch).
 */
export type Doc<
  TSchema extends Schema,
  TName extends keyof TSchema["tables"] & string,
> = {
  _id: Id<TName>;
  _createdAt: number;
} & ObjectOutput<TSchema["tables"][TName]["fields"]>;

/** Maps a schema to the document type of each of its tables. */
export type DataModelFromSchema<TSchema extends Schema> = {
  [TName in keyof TSchema["tables"] & string]: Doc<TSchema, TName>;
};

export interface DiffedIndex {
  readonly index: TableIndex;
  readonly tableName: string;
}

export interface SchemaDiff {
  readonly addedIndexes: readonly DiffedIndex[];
  readonly addedTables: SchemaTables;
  readonly hasChanges: boolean;
  /** Tables removed from the schema. Reported only — never auto-dropped. */
  readonly orphanedTables: string[];
  readonly removedIndexes: readonly DiffedIndex[];
  toStatements(): string[];
}

export function assertIdentifier(name: string, label: string): void {
  if (!IDENTIFIER_PATTERN.test(name)) {
    throw new SchemaError(
      `${label} "${name}" must start with a letter and contain only letters, numbers, and underscores`
    );
  }
}

export function assertTableName(name: string): void {
  if (name.startsWith("_")) {
    throw new SchemaError(`Table name "${name}" cannot start with "_"`);
  }

  assertIdentifier(name, "Table name");
}

export function assertFieldName(name: string): void {
  if (name.startsWith("_")) {
    throw new SchemaError(`Field name "${name}" cannot start with "_"`);
  }

  if (FILTER_LOGIC_FIELD_NAMES.has(name)) {
    throw new SchemaError(
      `Field name "${name}" is reserved for query filter logic`
    );
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
