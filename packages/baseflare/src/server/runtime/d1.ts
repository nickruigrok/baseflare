import { generateId } from "baseflare/values";

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
import { serialize } from "../db/serialize";
import {
  validateInsertData,
  validatePatchData,
  validateReplaceData,
} from "../db/write-validation";
import type { DatabaseWriter } from "../db/writer";
import type { Rules } from "../permissions/types";
import {
  type Schema,
  TABLE_VERSION_TABLE_NAME,
  type TableDefinition,
} from "../schema/types";

import {
  ConflictRuntimeError,
  coerceDatabaseError,
  coerceMalformedDocumentError,
  coerceValidationError,
  ensureSuccessfulD1Result,
  InternalRuntimeError,
  NotFoundRuntimeError,
  ValidationRuntimeError,
  withDatabaseErrorHandling,
} from "./errors";
import {
  assertCanDelete,
  assertCanInsert,
  assertCanUpdate,
  assertReadRulesConfigured,
  canReadDocument,
} from "./permissions";
import type {
  D1BindingValue,
  D1PreparedStatement,
  D1Result,
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
  readonly rowRevisions?: ReadonlyMap<string, ReadonlyMap<string, number>>;
  readonly tableVersions?: ReadonlyMap<string, number>;
}

export interface RuntimeScanPosition {
  readonly cursor: CursorPayload | null;
  readonly offset: number;
}

interface RuntimeQueryOptions<TContext> {
  readonly database: RuntimeDatabase;
  readonly getContext: () => TContext;
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

function buildCommitGuardConditions(
  guard: CommitGuard,
  options: {
    readonly ignoredTables?: ReadonlySet<string>;
  } = {}
): {
  readonly conditions: readonly string[];
  readonly params: readonly (string | number | null)[];
} {
  const conditions: string[] = [];
  const params: Array<string | number | null> = [];

  for (const [tableName, version] of guard.tableVersions ?? []) {
    if (options.ignoredTables?.has(tableName)) {
      continue;
    }

    appendGuardCondition(
      conditions,
      params,
      `EXISTS (SELECT 1 FROM ${TABLE_VERSION_TABLE_NAME} WHERE table_name = ? AND version = ?)`,
      [tableName, version]
    );
  }

  for (const [tableName, reads] of guard.rowRevisions ?? []) {
    if (options.ignoredTables?.has(tableName)) {
      continue;
    }

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
    if (options.ignoredTables?.has(tableName)) {
      continue;
    }

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
      throw new InternalRuntimeError(
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

    const documents: RuntimeDocument[] = [];
    let scanPosition: RuntimeScanPosition = { cursor, offset: 0 };
    let scannedBytes = 0;
    let scannedRows = 0;

    while (true) {
      const hasScanCursor = scanPosition.cursor !== null;
      const rows = await executeRowQuery<StoredDocumentRow>(
        this.options.database,
        buildRuntimeSelectQuery(this.options.tableName, this.state, {
          cursor: scanPosition.cursor ?? cursor,
          limit: QUERY_SCAN_CHUNK_SIZE,
          offset: hasScanCursor ? undefined : scanPosition.offset,
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

function ensureSingleChange(
  changes: number | undefined,
  operation: {
    readonly conflictOnZero?: boolean;
    readonly type: string;
  }
): void {
  if (changes === undefined) {
    throw new InternalRuntimeError(
      "D1 did not report a change count for the write operation"
    );
  }

  if (changes !== 1) {
    if (operation.conflictOnZero && changes === 0) {
      throw new ConflictRuntimeError("Document changed concurrently");
    }

    throw new InternalRuntimeError(
      `D1 write operation "${operation.type}" did not apply after its guard passed`
    );
  }
}

function assertTableVersionResult(
  result: D1Result,
  operation: { readonly tableName?: string }
): void {
  const row = result.results?.[0];
  if (typeof row?.version !== "number") {
    throw new InternalRuntimeError(
      `Missing internal table version row for "${operation.tableName ?? "unknown"}"`
    );
  }
}

export class D1DatabaseAdapter<TContext = unknown>
  implements DatabaseWriter<RuntimeDocument>
{
  private readonly database: RuntimeDatabase;
  private readonly getContext: () => TContext;
  private readonly rules?: Rules;
  private readonly schema: Schema;

  constructor(options: {
    database: RuntimeDatabase;
    getContext: () => TContext;
    rules?: Rules;
    schema: Schema;
  }) {
    this.database = options.database;
    this.getContext = options.getContext;
    this.rules = options.rules;
    this.schema = options.schema;
  }

  async get(tableName: string, id: string): Promise<RuntimeDocument | null> {
    assertKnownTable(this.schema, tableName);
    assertReadRulesConfigured(this.rules);
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
      rules: this.rules,
      schema: this.schema,
      tableName,
    });
  }

  async insert(
    tableName: string,
    doc: Record<string, unknown>
  ): Promise<string> {
    const table = assertKnownTable(this.schema, tableName);
    const validated = this.validateInsert(table, doc);
    await assertCanInsert(this.rules, tableName, this.getContext(), validated);

    const id = generateId();
    const serialized = serialize(validated);
    await this.runWriteBatch([
      createTableVersionAssertion(tableName),
      createGuardedTableVersionBump(
        tableName,
        createDirectWriteGuard(tableName, {
          insertId: id,
        })
      ),
      {
        type: "insert",
        sql: `INSERT INTO ${tableName} (_id, _data, _rev)
              SELECT ?, ?, 0 WHERE changes() = 1`,
        params: [id, serialized._data],
        requireSingleChange: true,
      },
    ]);

    return id;
  }

  async patch(
    tableName: string,
    id: string,
    partial: Record<string, unknown>
  ): Promise<void> {
    const table = assertKnownTable(this.schema, tableName);
    const existing = await this.getWritableDocument(tableName, id);
    const validated = this.validatePatch(table, existing.document, partial);
    await assertCanUpdate(
      this.rules,
      tableName,
      this.getContext(),
      existing.document,
      validated
    );

    const serialized = serialize(validated);
    await this.runWriteBatch([
      createTableVersionAssertion(tableName),
      createGuardedTableVersionBump(
        tableName,
        createDirectWriteGuard(tableName, {
          rowId: id,
          rev: existing.rev,
        })
      ),
      {
        type: "update",
        sql: `UPDATE ${tableName}
              SET _data = ?, _rev = _rev + 1
              WHERE _id = ? AND _rev = ? AND changes() = 1`,
        params: [serialized._data, id, existing.rev],
        requireSingleChange: true,
      },
    ]);
  }

  async replace(
    tableName: string,
    id: string,
    doc: Record<string, unknown>
  ): Promise<void> {
    const table = assertKnownTable(this.schema, tableName);
    const existing = await this.getWritableDocument(tableName, id);
    const validated = this.validateReplace(table, doc);
    await assertCanUpdate(
      this.rules,
      tableName,
      this.getContext(),
      existing.document,
      validated
    );

    const serialized = serialize(validated);
    await this.runWriteBatch([
      createTableVersionAssertion(tableName),
      createGuardedTableVersionBump(
        tableName,
        createDirectWriteGuard(tableName, {
          rowId: id,
          rev: existing.rev,
        })
      ),
      {
        type: "update",
        sql: `UPDATE ${tableName}
              SET _data = ?, _rev = _rev + 1
              WHERE _id = ? AND _rev = ? AND changes() = 1`,
        params: [serialized._data, id, existing.rev],
        requireSingleChange: true,
      },
    ]);
  }

  async delete(tableName: string, id: string): Promise<void> {
    assertKnownTable(this.schema, tableName);
    const existing = await this.getWritableDocument(tableName, id);
    await assertCanDelete(
      this.rules,
      tableName,
      this.getContext(),
      existing.document
    );

    await this.runWriteBatch([
      createTableVersionAssertion(tableName),
      createGuardedTableVersionBump(
        tableName,
        createDirectWriteGuard(tableName, {
          rowId: id,
          rev: existing.rev,
        })
      ),
      {
        type: "delete",
        sql: `DELETE FROM ${tableName}
              WHERE _id = ? AND _rev = ? AND changes() = 1`,
        params: [id, existing.rev],
        requireSingleChange: true,
      },
    ]);
  }

  private async getWritableDocument(
    tableName: string,
    id: string
  ): Promise<VersionedRuntimeDocument> {
    const existing = await fetchVersionedDocument(this.database, tableName, id);
    if (!existing) {
      throw new NotFoundRuntimeError(
        `Document "${id}" was not found in table "${tableName}"`
      );
    }

    return existing;
  }

  private validateInsert(
    table: Parameters<typeof validateInsertData>[0],
    value: Record<string, unknown>
  ): Record<string, unknown> {
    try {
      return validateInsertData(table, value);
    } catch (error) {
      return coerceValidationError(error, "Invalid insert document");
    }
  }

  private validatePatch(
    table: Parameters<typeof validatePatchData>[0],
    current: Record<string, unknown>,
    patch: Record<string, unknown>
  ): Record<string, unknown> {
    try {
      return validatePatchData(table, current, patch);
    } catch (error) {
      return coerceValidationError(error, "Invalid patch document");
    }
  }

  private validateReplace(
    table: Parameters<typeof validateReplaceData>[0],
    value: Record<string, unknown>
  ): Record<string, unknown> {
    try {
      return validateReplaceData(table, value);
    } catch (error) {
      return coerceValidationError(error, "Invalid replacement document");
    }
  }

  private async runWriteBatch(
    operations: readonly {
      readonly conflictOnZero?: boolean;
      readonly params: readonly (string | number | null)[];
      readonly requireSingleChange?: boolean;
      readonly sql: string;
      readonly tableName?: string;
      readonly type:
        | "assert-table-version"
        | "bump-table-version"
        | "delete"
        | "insert"
        | "update";
    }[]
  ): Promise<void> {
    const statements = operations.map((operation) =>
      bindStatement(this.database, operation.sql, operation.params)
    );
    const results = await withDatabaseErrorHandling(
      "Failed to commit D1 write",
      async () => this.database.batch(statements)
    );

    if (results.length !== operations.length) {
      throw new InternalRuntimeError(
        "D1 write batch returned an unexpected number of results"
      );
    }

    for (const [index, result] of results.entries()) {
      const operation = operations[index];
      ensureSuccessfulD1Result(result, "Failed to commit D1 write");
      if (!operation) {
        continue;
      }

      if (operation.type === "assert-table-version") {
        assertTableVersionResult(result, operation);
        continue;
      }

      if (operation.requireSingleChange) {
        ensureSingleChange(result.meta?.changes, operation);
      }
    }
  }
}

function createDirectWriteGuard(
  tableName: string,
  options:
    | {
        readonly insertId: string;
      }
    | {
        readonly rev: number;
        readonly rowId: string;
      }
): CommitGuard {
  if ("insertId" in options) {
    return { insertedIds: new Map([[tableName, new Set([options.insertId])]]) };
  }

  return {
    rowRevisions: new Map([
      [tableName, new Map([[options.rowId, options.rev]])],
    ]),
  };
}

function createTableVersionAssertion(tableName: string): {
  readonly params: readonly string[];
  readonly sql: string;
  readonly tableName: string;
  readonly type: "assert-table-version";
} {
  return {
    sql: `SELECT version FROM ${TABLE_VERSION_TABLE_NAME} WHERE table_name = ? LIMIT 1`,
    params: [tableName],
    tableName,
    type: "assert-table-version",
  };
}

export function createGuardedTableVersionBump(
  tableName: string,
  guard: CommitGuard,
  options: {
    readonly ignoredTables?: ReadonlySet<string>;
  } = {}
): {
  readonly conflictOnZero: true;
  readonly params: readonly (string | number | null)[];
  readonly requireSingleChange: true;
  readonly sql: string;
  readonly type: "bump-table-version";
} {
  const guardConditions = buildCommitGuardConditions(guard, options);
  const conditions = ["table_name = ?", ...guardConditions.conditions];

  return {
    sql: `UPDATE ${TABLE_VERSION_TABLE_NAME}
          SET version = version + 1
          WHERE ${conditions.join(" AND ")}`,
    params: [tableName, ...guardConditions.params],
    conflictOnZero: true,
    requireSingleChange: true,
    type: "bump-table-version",
  };
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
