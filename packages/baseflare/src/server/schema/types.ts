import {
  type Id,
  type ObjectOutput,
  SchemaError,
  type ValidatorShape,
} from "baseflare/values";

const IDENTIFIER_PATTERN = /^[A-Za-z][A-Za-z0-9_]*$/;
const FILTER_LOGIC_FIELD_NAMES = new Set(["AND", "OR", "NOT"]);
export const TABLE_VERSION_TABLE_NAME = "_bf_table_versions";
export const PARTITION_VERSION_TABLE_NAME = "_bf_partition_versions";
export const REALTIME_OUTBOX_TABLE_NAME = "_bf_realtime_outbox";
export const REALTIME_SHARD_CURSORS_TABLE_NAME = "_bf_realtime_shard_cursors";
export const REALTIME_SHARD_GENERATIONS_TABLE_NAME =
  "_bf_realtime_shard_generations";
export const REALTIME_AUTOSCALE_STATE_TABLE_NAME =
  "_bf_realtime_autoscale_state";

export interface TableIndexOptions {
  readonly partition?: boolean;
}

export interface TableIndex {
  readonly fields: readonly string[];
  readonly name: string;
  readonly partition?: boolean;
}

export interface TableDefinition<
  TFields extends ValidatorShape = ValidatorShape,
> {
  readonly fields: TFields;
  readonly indexes: readonly TableIndex[];
}

export interface TableBuilder<TFields extends ValidatorShape = ValidatorShape>
  extends TableDefinition<TFields> {
  index(
    name: string,
    fields: readonly string[],
    options?: TableIndexOptions
  ): TableBuilder<TFields>;
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

export interface SqlStatement {
  readonly params: readonly (number | string | null)[];
  readonly sql: string;
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
  return `CREATE TABLE ${tableName} (_id TEXT PRIMARY KEY, _data TEXT NOT NULL, _rev INTEGER NOT NULL DEFAULT 0 CHECK(_rev >= 0))`;
}

export function createIndexStatement(
  tableName: string,
  index: TableIndex,
  options: { ifNotExists?: boolean } = {}
): string {
  const expressions = index.fields
    .map((field) => `json_extract(_data, '$.${field}')`)
    .join(", ");
  const ifNotExists = options.ifNotExists ? " IF NOT EXISTS" : "";

  return `CREATE INDEX${ifNotExists} ${tableName}_${index.name} ON ${tableName} (${expressions})`;
}

export function createTableVersionStatements(
  tableNames: readonly string[]
): SqlStatement[] {
  const statements: SqlStatement[] = [
    {
      sql: `CREATE TABLE IF NOT EXISTS ${TABLE_VERSION_TABLE_NAME} (table_name TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0))`,
      params: [],
    },
  ];

  for (const tableName of tableNames) {
    statements.push({
      sql: `INSERT OR IGNORE INTO ${TABLE_VERSION_TABLE_NAME} (table_name, version) VALUES (?, 0)`,
      params: [tableName],
    });
  }

  return statements;
}

export function createPartitionVersionStatements(): SqlStatement[] {
  return [
    {
      sql: `CREATE TABLE IF NOT EXISTS ${PARTITION_VERSION_TABLE_NAME} (table_name TEXT NOT NULL, partition_key TEXT NOT NULL, partition_value TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0), PRIMARY KEY (table_name, partition_key, partition_value))`,
      params: [],
    },
  ];
}

export function createRealtimeOutboxStatements(): SqlStatement[] {
  return [
    {
      sql: `CREATE TABLE IF NOT EXISTS ${REALTIME_OUTBOX_TABLE_NAME} (sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, created_at INTEGER NOT NULL, tables TEXT NOT NULL, partitions TEXT NOT NULL)`,
      params: [],
    },
    {
      sql: `CREATE INDEX IF NOT EXISTS ${REALTIME_OUTBOX_TABLE_NAME}_created_at ON ${REALTIME_OUTBOX_TABLE_NAME} (created_at)`,
      params: [],
    },
  ];
}

export function createRealtimeShardMetadataStatements(): SqlStatement[] {
  return [
    {
      sql: `CREATE TABLE IF NOT EXISTS ${REALTIME_SHARD_GENERATIONS_TABLE_NAME} (generation_id INTEGER PRIMARY KEY, subscription_shard_count INTEGER NOT NULL CHECK(subscription_shard_count > 0), status TEXT NOT NULL CHECK(status IN ('active', 'draining', 'retired')), created_at INTEGER NOT NULL, drain_after INTEGER)`,
      params: [],
    },
    {
      sql: `INSERT OR IGNORE INTO ${REALTIME_SHARD_GENERATIONS_TABLE_NAME} (generation_id, subscription_shard_count, status, created_at, drain_after) VALUES (1, 1, 'active', 0, NULL)`,
      params: [],
    },
    {
      sql: `CREATE TABLE IF NOT EXISTS ${REALTIME_SHARD_CURSORS_TABLE_NAME} (shard_name TEXT PRIMARY KEY, generation_id INTEGER NOT NULL, last_processed_outbox_sequence INTEGER, updated_at INTEGER NOT NULL)`,
      params: [],
    },
    {
      sql: `CREATE TABLE IF NOT EXISTS ${REALTIME_AUTOSCALE_STATE_TABLE_NAME} (id INTEGER PRIMARY KEY CHECK(id = 1), scale_up_started_at INTEGER, scale_down_started_at INTEGER, updated_at INTEGER NOT NULL)`,
      params: [],
    },
    {
      sql: `INSERT OR IGNORE INTO ${REALTIME_AUTOSCALE_STATE_TABLE_NAME} (id, scale_up_started_at, scale_down_started_at, updated_at) VALUES (1, NULL, NULL, 0)`,
      params: [],
    },
  ];
}
