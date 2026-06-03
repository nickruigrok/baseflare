import {
  buildCursorPredicate,
  type CursorPayload,
  decodeCursor,
  encodeCursor,
} from "../db/cursor";
import { deserialize } from "../db/deserialize";
import {
  assertQueryField,
  compileFilter,
  type FilterObject,
  type FilterValue,
} from "../db/filters";
import {
  assertTableIdentifier,
  buildOrderClause,
  type QueryState,
} from "../db/query-builder";
import type { QueryBuilder, QueryOrderDirection } from "../db/reader";
import type { Rules } from "../permissions/types";
import {
  PARTITION_VERSION_TABLE_NAME,
  type Schema,
  TABLE_VERSION_TABLE_NAME,
  type TableDefinition,
} from "../schema/types";

import {
  coerceDatabaseError,
  coerceMalformedDocumentError,
  ensureSuccessfulD1Result,
  InternalRuntimeError,
  NotFoundRuntimeError,
  ValidationRuntimeError,
  withDatabaseErrorHandling,
} from "./errors";
import {
  getPartitionReadTarget,
  type PartitionReadTarget,
} from "./partitioning";
import { assertReadRulesConfigured, canReadDocument } from "./permissions";
import type {
  D1BindingValue,
  D1PreparedStatement,
  RuntimeDatabase,
} from "./types";

export type RuntimeDocument = Record<string, unknown> & {
  _createdAt: number;
  _id: string;
};

export interface StoredDocumentRow extends Record<string, unknown> {
  readonly _data: string;
  readonly _id: string;
  readonly _rev: number;
}

export interface VersionedRuntimeDocument {
  readonly document: RuntimeDocument;
  readonly rev: number;
}

export interface CommitGuard {
  readonly insertedIds?: ReadonlyMap<string, ReadonlySet<string>>;
  readonly partitionVersions?: readonly PartitionVersionRead[];
  readonly rowRevisions?: ReadonlyMap<string, ReadonlyMap<string, number>>;
  readonly tableVersions?: ReadonlyMap<string, number>;
}

export interface PartitionVersionKey {
  readonly partitionKey: string;
  readonly partitionValue: string;
  readonly tableName: string;
}

export interface PartitionVersionRead extends PartitionVersionKey {
  readonly version: number;
}

export interface RuntimeScanPosition {
  readonly cursor: CursorPayload | null;
  readonly offset: number;
}

export interface RuntimeScanQueryOptions {
  readonly cursor: CursorPayload | null;
  readonly offset?: number;
}

export interface RuntimeReadObserver {
  onPartitionRead(partition: PartitionReadTarget): void;
  onTableRead(tableName: string): void;
}

interface RuntimeQueryOptions<TContext> {
  readonly database: RuntimeDatabase;
  readonly getContext: () => TContext;
  readonly readObserver?: RuntimeReadObserver;
  readonly rules?: Rules;
  readonly schema: Schema;
  readonly tableName: string;
}

type D1PrepareDatabase = Pick<RuntimeDatabase, "prepare">;

const QUERY_SCAN_CHUNK_SIZE = 256;
const QUERY_SCAN_BYTE_LIMIT = 5_000_000;
const QUERY_SCAN_ROW_LIMIT = 20_000;
export const DEFAULT_SCAN_BUDGET_MESSAGE =
  "Query exceeded the internal scan budget; add a more selective filter";
export const COUNT_SCAN_BUDGET_MESSAGE =
  "Count exceeded the internal scan budget; add a more selective filter before count()";

function appendGuardCondition(
  conditions: string[],
  params: Array<string | number | null>,
  condition: string,
  conditionParams: readonly (string | number | null)[]
): void {
  conditions.push(condition);
  params.push(...conditionParams);
}

function buildCommitGuardConditions(guard: CommitGuard): {
  readonly conditions: readonly string[];
  readonly params: readonly (string | number | null)[];
} {
  const conditions: string[] = [];
  const params: Array<string | number | null> = [];

  for (const [tableName, version] of guard.tableVersions ?? []) {
    appendGuardCondition(
      conditions,
      params,
      `EXISTS (SELECT 1 FROM ${TABLE_VERSION_TABLE_NAME} WHERE table_name = ? AND version = ?)`,
      [tableName, version]
    );
  }

  for (const read of guard.partitionVersions ?? []) {
    if (read.version === 0) {
      appendGuardCondition(
        conditions,
        params,
        `(NOT EXISTS (SELECT 1 FROM ${PARTITION_VERSION_TABLE_NAME} WHERE table_name = ? AND partition_key = ? AND partition_value = ?) OR EXISTS (SELECT 1 FROM ${PARTITION_VERSION_TABLE_NAME} WHERE table_name = ? AND partition_key = ? AND partition_value = ? AND version = 0))`,
        [
          read.tableName,
          read.partitionKey,
          read.partitionValue,
          read.tableName,
          read.partitionKey,
          read.partitionValue,
        ]
      );
      continue;
    }

    appendGuardCondition(
      conditions,
      params,
      `EXISTS (SELECT 1 FROM ${PARTITION_VERSION_TABLE_NAME} WHERE table_name = ? AND partition_key = ? AND partition_value = ? AND version = ?)`,
      [read.tableName, read.partitionKey, read.partitionValue, read.version]
    );
  }

  for (const [tableName, reads] of guard.rowRevisions ?? []) {
    for (const [id, rev] of reads) {
      appendGuardCondition(
        conditions,
        params,
        `EXISTS (SELECT 1 FROM ${tableName} WHERE _id = ? AND _rev = ?)`,
        [id, rev]
      );
    }
  }

  for (const [tableName, ids] of guard.insertedIds ?? []) {
    for (const id of ids) {
      appendGuardCondition(
        conditions,
        params,
        `NOT EXISTS (SELECT 1 FROM ${tableName} WHERE _id = ?)`,
        [id]
      );
    }
  }

  return { conditions, params };
}

export function bindStatement(
  database: D1PrepareDatabase,
  sql: string,
  params: readonly (string | number | null)[]
): D1PreparedStatement {
  try {
    return database.prepare(sql).bind(...(params as D1BindingValue[]));
  } catch (error) {
    coerceDatabaseError(error, `Failed to prepare D1 statement "${sql}"`);
  }
}

export function assertKnownTable(
  schema: Schema,
  tableName: string
): TableDefinition {
  const table = Object.hasOwn(schema.tables, tableName)
    ? schema.tables[tableName]
    : undefined;
  if (!table) {
    throw new NotFoundRuntimeError(`Unknown table "${tableName}"`);
  }

  return table;
}

export async function executeRowQuery<TRow extends Record<string, unknown>>(
  database: D1PrepareDatabase,
  query: {
    readonly params: readonly (string | number | null)[];
    readonly sql: string;
  }
): Promise<readonly TRow[]> {
  const result = await withDatabaseErrorHandling(
    `Failed to run D1 query "${query.sql}"`,
    async () => bindStatement(database, query.sql, query.params).all<TRow>()
  );
  return (
    ensureSuccessfulD1Result(result, `Failed to run D1 query "${query.sql}"`)
      .results ?? []
  );
}

export function deserializeRuntimeDocument(
  tableName: string,
  row: { readonly _data: string; readonly _id: string }
): RuntimeDocument {
  try {
    return deserialize(row) as RuntimeDocument;
  } catch (error) {
    coerceMalformedDocumentError(error, tableName, row._id);
  }
}

export function deserializeVersionedRuntimeDocument(
  tableName: string,
  row: StoredDocumentRow
): VersionedRuntimeDocument {
  return {
    document: deserializeRuntimeDocument(tableName, row),
    rev: row._rev,
  };
}

export function fetchStoredDocument(
  database: D1PrepareDatabase,
  tableName: string,
  id: string
): Promise<StoredDocumentRow | null> {
  const sql = `SELECT _id, _data, _rev FROM ${tableName} WHERE _id = ? LIMIT 1`;
  return withDatabaseErrorHandling(
    `Failed to fetch document "${id}" from table "${tableName}"`,
    async () => bindStatement(database, sql, [id]).first<StoredDocumentRow>()
  );
}

export async function fetchVersionedDocument(
  database: D1PrepareDatabase,
  tableName: string,
  id: string
): Promise<VersionedRuntimeDocument | null> {
  const row = await fetchStoredDocument(database, tableName, id);
  return row ? deserializeVersionedRuntimeDocument(tableName, row) : null;
}

function createBaseQueryState(): QueryState {
  return { order: { field: "_id", direction: "asc" } };
}

function assertDirection(value: string): asserts value is QueryOrderDirection {
  if (value !== "asc" && value !== "desc") {
    throw new Error(
      `Order direction must be "asc" or "desc", received "${value}"`
    );
  }
}

function normalizeOrderField(field: string): string {
  if (field === "_id" || field === "_createdAt") {
    return "_id";
  }

  assertQueryField(field);
  return field;
}

function mergeFilters(
  left: FilterObject | undefined,
  right: FilterObject
): FilterObject {
  return left ? { AND: [left, right] } : right;
}

function toScalarCursorValue(value: unknown): FilterValue | undefined {
  if (value === undefined || value === null) {
    return null;
  }

  const type = typeof value;
  if (type === "boolean" || type === "number" || type === "string") {
    return value as FilterValue;
  }

  return undefined;
}

export function buildRuntimeSelectQuery(
  tableName: string,
  state: QueryState,
  options: {
    readonly cursor?: CursorPayload | null;
    readonly limit: number;
    readonly offset?: number;
  }
): { params: Array<string | number | null>; sql: string } {
  assertTableIdentifier(tableName);
  const clauses: string[] = [];
  const params: Array<string | number | null> = [];

  if (state.filter) {
    const compiled = compileFilter(state.filter);
    clauses.push(compiled.sql);
    params.push(...compiled.params);
  }

  if (options.cursor) {
    const predicate = buildCursorPredicate(state.order, options.cursor);
    clauses.push(predicate.sql);
    params.push(...predicate.params);
  }

  let sql = `SELECT _id, _data, _rev FROM ${tableName}`;
  if (clauses.length > 0) {
    sql += ` WHERE ${clauses.join(" AND ")}`;
  }

  sql += ` ${buildOrderClause(state.order)} LIMIT ?`;
  params.push(options.limit);

  if (options.offset !== undefined && options.offset > 0) {
    sql += " OFFSET ?";
    params.push(options.offset);
  }

  return { sql, params };
}

export function getNextRuntimeScanPosition(
  tableName: string,
  state: QueryState,
  current: RuntimeScanPosition,
  rows: readonly StoredDocumentRow[]
): RuntimeScanPosition {
  const offset = current.offset + rows.length;
  const row = rows.at(-1);
  if (!row) {
    return current;
  }

  if (state.order.field === "_id") {
    return {
      cursor: {
        id: row._id,
        orderDirection: state.order.direction,
        orderField: "_id",
      },
      offset,
    };
  }

  const document = deserializeRuntimeDocument(tableName, row);
  const value = toScalarCursorValue(document[state.order.field]);
  if (value === undefined) {
    return { cursor: null, offset };
  }

  return {
    cursor: {
      id: row._id,
      orderDirection: state.order.direction,
      orderField: state.order.field,
      v: value,
    },
    offset,
  };
}

export function getRuntimeScanQueryOptions(
  position: RuntimeScanPosition,
  baseCursor: CursorPayload | null
): RuntimeScanQueryOptions {
  if (position.cursor) {
    return { cursor: position.cursor };
  }

  // Non-scalar ordered values cannot produce a safe keyset cursor, so keep the
  // caller's cursor as the base boundary and advance later chunks by offset.
  return { cursor: baseCursor, offset: position.offset };
}

class D1RuntimeQueryBuilder<TContext> implements QueryBuilder<RuntimeDocument> {
  private readonly options: RuntimeQueryOptions<TContext>;
  private readonly state: QueryState;

  constructor(
    options: RuntimeQueryOptions<TContext>,
    state: QueryState = createBaseQueryState()
  ) {
    this.options = options;
    this.state = state;
  }

  filter(filter: FilterObject): QueryBuilder<RuntimeDocument> {
    return this.clone({ filter: mergeFilters(this.state.filter, filter) });
  }

  order(direction: QueryOrderDirection): QueryBuilder<RuntimeDocument>;
  order(
    field: string,
    direction: QueryOrderDirection
  ): QueryBuilder<RuntimeDocument>;
  order(
    fieldOrDirection: string,
    maybeDirection?: QueryOrderDirection
  ): QueryBuilder<RuntimeDocument> {
    if (maybeDirection === undefined) {
      assertDirection(fieldOrDirection);
      return this.clone({
        order: { field: "_id", direction: fieldOrDirection },
      });
    }

    assertDirection(maybeDirection);
    return this.clone({
      order: {
        field: normalizeOrderField(fieldOrDirection),
        direction: maybeDirection,
      },
    });
  }

  limit(limit: number): QueryBuilder<RuntimeDocument> {
    if (!Number.isInteger(limit) || limit < 0) {
      throw new Error("Query limits must be a non-negative integer");
    }

    return this.clone({ limit });
  }

  collect(): Promise<RuntimeDocument[]> {
    return this.collectReadable(this.state.limit, null);
  }

  async first(): Promise<RuntimeDocument | null> {
    const documents = await this.collectReadable(1, null);
    return documents[0] ?? null;
  }

  async unique(): Promise<RuntimeDocument> {
    const documents = await this.collectReadable(2, null);
    if (documents.length === 0) {
      throw new NotFoundRuntimeError("Document not found");
    }

    if (documents.length > 1) {
      throw new ValidationRuntimeError(
        `Expected exactly one document from "${this.options.tableName}", received ${documents.length}`
      );
    }

    const document = documents[0];
    if (!document) {
      throw new InternalRuntimeError(
        "Expected a document but none was returned"
      );
    }

    return document;
  }

  take(count: number): Promise<RuntimeDocument[]> {
    return this.limit(count).collect();
  }

  async count(): Promise<number> {
    return (
      await this.collectReadable(undefined, null, COUNT_SCAN_BUDGET_MESSAGE)
    ).length;
  }

  async paginate(options: {
    cursor: string | null;
    numItems: number;
  }): Promise<{
    continueCursor: string;
    isDone: boolean;
    page: RuntimeDocument[];
  }> {
    if (!Number.isInteger(options.numItems) || options.numItems <= 0) {
      throw new Error("Pagination requires a positive integer numItems value");
    }

    const cursor = options.cursor
      ? decodeCursor(options.cursor, this.state.order)
      : null;
    const documents = await this.collectReadable(options.numItems + 1, cursor);
    const page = documents.slice(0, options.numItems);
    const lastDocument = page.at(-1);

    return {
      continueCursor: lastDocument
        ? encodeCursor(this.state.order, lastDocument)
        : (options.cursor ?? ""),
      isDone: documents.length <= options.numItems,
      page,
    };
  }

  private clone(partial: Partial<QueryState>): D1RuntimeQueryBuilder<TContext> {
    return new D1RuntimeQueryBuilder(this.options, {
      ...this.state,
      ...partial,
    });
  }

  private async collectReadable(
    readableLimit: number | undefined,
    cursor: CursorPayload | null,
    scanBudgetMessage = DEFAULT_SCAN_BUDGET_MESSAGE
  ): Promise<RuntimeDocument[]> {
    assertReadRulesConfigured(this.options.rules);
    const table = assertKnownTable(this.options.schema, this.options.tableName);
    const partition = getPartitionReadTarget(
      this.options.tableName,
      table,
      this.state
    );
    if (partition) {
      this.options.readObserver?.onPartitionRead(partition);
    } else {
      this.options.readObserver?.onTableRead(this.options.tableName);
    }

    const documents: RuntimeDocument[] = [];
    let scanPosition: RuntimeScanPosition = { cursor, offset: 0 };
    let scannedBytes = 0;
    let scannedRows = 0;

    while (true) {
      const scanOptions = getRuntimeScanQueryOptions(scanPosition, cursor);
      const rows = await executeRowQuery<StoredDocumentRow>(
        this.options.database,
        buildRuntimeSelectQuery(this.options.tableName, this.state, {
          cursor: scanOptions.cursor,
          limit: QUERY_SCAN_CHUNK_SIZE,
          offset: scanOptions.offset,
        })
      );
      if (rows.length === 0) {
        break;
      }

      scanPosition = getNextRuntimeScanPosition(
        this.options.tableName,
        this.state,
        scanPosition,
        rows
      );

      for (const row of rows) {
        scannedRows += 1;
        scannedBytes += row._data.length;
        assertWithinScanBudget(scannedRows, scannedBytes, scanBudgetMessage);

        const document = deserializeRuntimeDocument(
          this.options.tableName,
          row
        );
        if (
          await canReadDocument(
            this.options.rules,
            this.options.tableName,
            this.options.getContext(),
            document
          )
        ) {
          documents.push(document);
        }

        if (readableLimit !== undefined && documents.length >= readableLimit) {
          return documents;
        }
      }

      if (rows.length < QUERY_SCAN_CHUNK_SIZE) {
        break;
      }
    }

    return documents;
  }
}

export function assertWithinScanBudget(
  scannedRows: number,
  scannedBytes: number,
  message = DEFAULT_SCAN_BUDGET_MESSAGE
): void {
  if (
    scannedRows > QUERY_SCAN_ROW_LIMIT ||
    scannedBytes > QUERY_SCAN_BYTE_LIMIT
  ) {
    throw new ValidationRuntimeError(message);
  }
}

export class D1DatabaseAdapter<TContext = unknown> {
  private readonly database: RuntimeDatabase;
  private readonly getContext: () => TContext;
  private readonly readObserver?: RuntimeReadObserver;
  private readonly rules?: Rules;
  private readonly schema: Schema;

  constructor(options: {
    database: RuntimeDatabase;
    getContext: () => TContext;
    readObserver?: RuntimeReadObserver;
    rules?: Rules;
    schema: Schema;
  }) {
    this.database = options.database;
    this.getContext = options.getContext;
    this.readObserver = options.readObserver;
    this.rules = options.rules;
    this.schema = options.schema;
  }

  async get(tableName: string, id: string): Promise<RuntimeDocument | null> {
    assertKnownTable(this.schema, tableName);
    assertReadRulesConfigured(this.rules);
    this.readObserver?.onTableRead(tableName);
    const versioned = await fetchVersionedDocument(
      this.database,
      tableName,
      id
    );
    if (!versioned) {
      return null;
    }

    return (await canReadDocument(
      this.rules,
      tableName,
      this.getContext(),
      versioned.document
    ))
      ? versioned.document
      : null;
  }

  query(tableName: string): QueryBuilder<RuntimeDocument> {
    assertKnownTable(this.schema, tableName);
    return new D1RuntimeQueryBuilder({
      database: this.database,
      getContext: this.getContext,
      readObserver: this.readObserver,
      rules: this.rules,
      schema: this.schema,
      tableName,
    });
  }
}

export function createGuardedTableVersionBumps(
  tableNames: readonly string[],
  guard: CommitGuard
): {
  readonly conflictOnZero: true;
  readonly expectedChanges: number;
  readonly params: readonly (string | number | null)[];
  readonly sql: string;
  readonly type: "bump-table-versions";
} {
  if (tableNames.length === 0) {
    throw new InternalRuntimeError(
      "Guarded table-version bump requires at least one table"
    );
  }

  const guardConditions = buildCommitGuardConditions(guard);
  const placeholders = tableNames.map(() => "?").join(", ");
  const conditions = [
    `table_name IN (${placeholders})`,
    ...guardConditions.conditions,
  ];

  return {
    sql: `UPDATE ${TABLE_VERSION_TABLE_NAME}
          SET version = version + 1
          WHERE ${conditions.join(" AND ")}`,
    params: [...tableNames, ...guardConditions.params],
    conflictOnZero: true,
    expectedChanges: tableNames.length,
    type: "bump-table-versions",
  };
}

export function createEnsurePartitionVersionRows(
  partitions: readonly PartitionVersionKey[]
): {
  readonly params: readonly (string | number | null)[];
  readonly sql: string;
  readonly type: "ensure-partition-versions";
} {
  if (partitions.length === 0) {
    throw new InternalRuntimeError(
      "Partition version row creation requires at least one partition"
    );
  }

  const values = partitions.map(() => "(?, ?, ?, 0)").join(", ");
  return {
    sql: `INSERT OR IGNORE INTO ${PARTITION_VERSION_TABLE_NAME} (table_name, partition_key, partition_value, version) VALUES ${values}`,
    params: partitions.flatMap((partition) => [
      partition.tableName,
      partition.partitionKey,
      partition.partitionValue,
    ]),
    type: "ensure-partition-versions",
  };
}

export function createGuardedPartitionVersionBumps(
  partitions: readonly PartitionVersionKey[],
  guard: CommitGuard,
  expectedPreviousChanges: number
): {
  readonly expectedChanges: number;
  readonly params: readonly (string | number | null)[];
  readonly sql: string;
  readonly type: "bump-partition-versions";
} {
  if (partitions.length === 0) {
    throw new InternalRuntimeError(
      "Guarded partition-version bump requires at least one partition"
    );
  }

  const guardConditions = buildCommitGuardConditions(guard);
  const partitionConditions = partitions.map(
    () => "(table_name = ? AND partition_key = ? AND partition_value = ?)"
  );
  const conditions = [
    `(${partitionConditions.join(" OR ")})`,
    "changes() = ?",
    ...guardConditions.conditions,
  ];

  return {
    sql: `UPDATE ${PARTITION_VERSION_TABLE_NAME}
          SET version = version + 1
          WHERE ${conditions.join(" AND ")}`,
    params: [
      ...partitions.flatMap((partition) => [
        partition.tableName,
        partition.partitionKey,
        partition.partitionValue,
      ]),
      expectedPreviousChanges,
      ...guardConditions.params,
    ],
    expectedChanges: partitions.length,
    type: "bump-partition-versions",
  };
}
