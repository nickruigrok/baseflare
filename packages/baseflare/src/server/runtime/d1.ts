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
} from "../db/filters";
import { buildOrderClause, type QueryState } from "../db/query-builder";
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
  ensureSuccessfulD1Result,
  InternalRuntimeError,
  NotFoundRuntimeError,
  withDatabaseErrorHandling,
} from "./errors";
import {
  assertCanDelete,
  assertCanInsert,
  assertCanUpdate,
  canReadDocument,
} from "./permissions";
import type { D1BindingValue, D1Database, D1PreparedStatement } from "./types";

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

interface RuntimeQueryOptions<TContext> {
  readonly database: D1Database;
  readonly getContext: () => TContext;
  readonly rules?: Rules;
  readonly schema: Schema;
  readonly tableName: string;
}

const QUERY_SCAN_CHUNK_SIZE = 256;
const QUERY_SCAN_BYTE_LIMIT = 5_000_000;
const QUERY_SCAN_ROW_LIMIT = 20_000;

export function bindStatement(
  database: D1Database,
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
  database: D1Database,
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
  database: D1Database,
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
  database: D1Database,
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

export function buildRuntimeSelectQuery(
  tableName: string,
  state: QueryState,
  options: {
    readonly cursor?: CursorPayload | null;
    readonly limit: number;
    readonly offset?: number;
  }
): { params: Array<string | number | null>; sql: string } {
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
    if (documents.length !== 1) {
      throw new InternalRuntimeError(
        `Expected exactly one document, received ${documents.length}`
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
    return (await this.collectReadable(undefined, null)).length;
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
    cursor: CursorPayload | null
  ): Promise<RuntimeDocument[]> {
    if (!this.options.rules) {
      return [];
    }

    const documents: RuntimeDocument[] = [];
    let offset = 0;
    let scannedBytes = 0;
    let scannedRows = 0;

    while (true) {
      const rows = await executeRowQuery<StoredDocumentRow>(
        this.options.database,
        buildRuntimeSelectQuery(this.options.tableName, this.state, {
          cursor,
          limit: QUERY_SCAN_CHUNK_SIZE,
          offset,
        })
      );
      if (rows.length === 0) {
        break;
      }

      offset += rows.length;
      for (const row of rows) {
        scannedRows += 1;
        scannedBytes += row._data.length;
        assertWithinScanBudget(scannedRows, scannedBytes);

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
  scannedBytes: number
): void {
  if (
    scannedRows > QUERY_SCAN_ROW_LIMIT ||
    scannedBytes > QUERY_SCAN_BYTE_LIMIT
  ) {
    throw new InternalRuntimeError("Query exceeded the internal scan budget");
  }
}

function ensureSingleChange(changes: number | undefined): void {
  if (changes !== undefined && changes !== 1) {
    throw new ConflictRuntimeError("Document changed concurrently");
  }
}

export class D1DatabaseAdapter<TContext = unknown>
  implements DatabaseWriter<RuntimeDocument>
{
  private readonly database: D1Database;
  private readonly getContext: () => TContext;
  private readonly rules?: Rules;
  private readonly schema: Schema;

  constructor(options: {
    database: D1Database;
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
    const validated = validateInsertData(table, doc);
    await assertCanInsert(this.rules, tableName, this.getContext(), validated);

    const id = generateId();
    const serialized = serialize(validated);
    await this.runWriteBatch([
      {
        sql: `INSERT INTO ${tableName} (_id, _data, _rev) VALUES (?, ?, 0)`,
        params: [id, serialized._data],
      },
      createTableVersionBump(tableName),
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
    const validated = validatePatchData(table, existing.document, partial);
    await assertCanUpdate(
      this.rules,
      tableName,
      this.getContext(),
      existing.document,
      validated
    );

    const serialized = serialize(validated);
    await this.runWriteBatch([
      {
        sql: `UPDATE ${tableName} SET _data = ?, _rev = _rev + 1 WHERE _id = ? AND _rev = ?`,
        params: [serialized._data, id, existing.rev],
        requireSingleChange: true,
      },
      createTableVersionBump(tableName),
    ]);
  }

  async replace(
    tableName: string,
    id: string,
    doc: Record<string, unknown>
  ): Promise<void> {
    const table = assertKnownTable(this.schema, tableName);
    const existing = await this.getWritableDocument(tableName, id);
    const validated = validateReplaceData(table, doc);
    await assertCanUpdate(
      this.rules,
      tableName,
      this.getContext(),
      existing.document,
      validated
    );

    const serialized = serialize(validated);
    await this.runWriteBatch([
      {
        sql: `UPDATE ${tableName} SET _data = ?, _rev = _rev + 1 WHERE _id = ? AND _rev = ?`,
        params: [serialized._data, id, existing.rev],
        requireSingleChange: true,
      },
      createTableVersionBump(tableName),
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
      {
        sql: `DELETE FROM ${tableName} WHERE _id = ? AND _rev = ?`,
        params: [id, existing.rev],
        requireSingleChange: true,
      },
      createTableVersionBump(tableName),
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

  private async runWriteBatch(
    operations: readonly {
      readonly params: readonly (string | number | null)[];
      readonly requireSingleChange?: boolean;
      readonly sql: string;
    }[]
  ): Promise<void> {
    const statements = operations.map((operation) =>
      bindStatement(this.database, operation.sql, operation.params)
    );
    const results = await withDatabaseErrorHandling(
      "Failed to commit D1 write",
      async () => this.database.batch(statements)
    );

    for (const [index, result] of results.entries()) {
      ensureSuccessfulD1Result(result, "Failed to commit D1 write");
      if (operations[index]?.requireSingleChange) {
        ensureSingleChange(result.meta?.changes);
      }
    }
  }
}

export function createTableVersionBump(tableName: string): {
  readonly params: readonly string[];
  readonly requireSingleChange: true;
  readonly sql: string;
} {
  return {
    sql: `UPDATE ${TABLE_VERSION_TABLE_NAME} SET version = version + 1 WHERE table_name = ?`,
    params: [tableName],
    requireSingleChange: true,
  };
}
