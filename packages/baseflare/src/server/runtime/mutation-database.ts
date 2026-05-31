import { generateId, getCreatedMsFromId } from "baseflare/values";

import { type CursorPayload, decodeCursor, encodeCursor } from "../db/cursor";
import {
  assertQueryField,
  compareSqliteJsonValues,
  type FilterObject,
  matchesFilter,
} from "../db/filters";
import type { QueryState } from "../db/query-builder";
import type { QueryBuilder, QueryOrderDirection } from "../db/reader";
import { serialize } from "../db/serialize";
import {
  validateInsertData,
  validatePatchData,
  validateReplaceData,
} from "../db/write-validation";
import type { DatabaseWriter } from "../db/writer";
import type { MutationCtx } from "../functions/types";
import type { Rules } from "../permissions/types";
import { type Schema, TABLE_VERSION_TABLE_NAME } from "../schema/types";

import {
  assertKnownTable,
  assertWithinScanBudget,
  bindStatement,
  buildRuntimeSelectQuery,
  type CommitGuard,
  createGuardedTableVersionBumps,
  deserializeRuntimeDocument,
  deserializeVersionedRuntimeDocument,
  executeRowQuery,
  getNextRuntimeScanPosition,
  type RuntimeDocument,
  type RuntimeScanPosition,
  type StoredDocumentRow,
} from "./d1";
import {
  ConflictRuntimeError,
  coerceDatabaseError,
  coerceValidationError,
  ensureSuccessfulD1Result,
  InternalRuntimeError,
  NotFoundRuntimeError,
  withDatabaseErrorHandling,
} from "./errors";
import { logRuntimeEvent } from "./logging";
import {
  assertCanDelete,
  assertCanInsert,
  assertCanUpdate,
  canReadDocument,
} from "./permissions";
import type { D1DatabaseSession, D1Result, RuntimeDatabase } from "./types";

type SessionDatabase = Pick<RuntimeDatabase, "batch" | "prepare">;

interface PendingMutationWrite {
  readonly baseRev?: number;
  readonly document?: RuntimeDocument;
  readonly serializedData?: string;
  readonly type: "delete" | "insert" | "update";
}

interface TableVersionReadResult<TRow extends Record<string, unknown>> {
  readonly rows: readonly TRow[];
  readonly version: unknown;
}

interface ScanBudget {
  scannedBytes: number;
  scannedRows: number;
}

type CommitOperation =
  | {
      readonly conflictOnZero: true;
      readonly expectedChanges: number;
      readonly params: readonly (string | number | null)[];
      readonly sql: string;
      readonly type: "bump-table-versions";
    }
  | {
      readonly expectedChanges: number;
      readonly params: readonly (string | number | null)[];
      readonly sql: string;
      readonly type: "delete" | "insert" | "update";
    };

const MUTATION_QUERY_CHUNK_SIZE = 256;

function createBaseQueryState(): QueryState {
  return { order: { field: "_id", direction: "asc" } };
}

function getMutationQueryChunkSize(
  state: QueryState,
  shadowedCount: number
): number {
  if (state.limit === undefined) {
    return MUTATION_QUERY_CHUNK_SIZE;
  }

  if (state.limit === 0) {
    return 0;
  }

  return Math.min(state.limit + shadowedCount, MUTATION_QUERY_CHUNK_SIZE);
}

function mergeFilters(
  left: FilterObject | undefined,
  right: FilterObject
): FilterObject {
  return left ? { AND: [left, right] } : right;
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

function hasCommittedEffect(write: PendingMutationWrite): boolean {
  return (
    write.type === "insert" ||
    write.type === "update" ||
    write.baseRev !== undefined
  );
}

function compareDocuments(
  left: RuntimeDocument,
  right: RuntimeDocument,
  state: QueryState
): number {
  let comparison =
    state.order.field === "_id"
      ? compareSqliteJsonValues(left._id, right._id)
      : compareSqliteJsonValues(
          left[state.order.field],
          right[state.order.field]
        );

  if (comparison === 0 && state.order.field !== "_id") {
    comparison = compareSqliteJsonValues(left._id, right._id);
  }

  return state.order.direction === "asc" ? comparison : -comparison;
}

function insertLimitedDocument(
  documents: RuntimeDocument[],
  document: RuntimeDocument,
  state: QueryState
): void {
  if (state.limit === undefined) {
    documents.push(document);
    return;
  }

  if (state.limit === 0) {
    return;
  }

  const insertIndex = documents.findIndex(
    (existing) => compareDocuments(document, existing, state) < 0
  );

  if (insertIndex === -1) {
    if (documents.length < state.limit) {
      documents.push(document);
    }
    return;
  }

  documents.splice(insertIndex, 0, document);
  if (documents.length > state.limit) {
    documents.pop();
  }
}

function isAfterCursor(
  document: RuntimeDocument,
  cursor: CursorPayload
): boolean {
  const cursorDocument: RuntimeDocument = {
    _id: cursor.id,
    _createdAt: getCreatedMsFromId(cursor.id),
    ...(cursor.v === undefined ? {} : { [cursor.orderField]: cursor.v }),
  };

  return (
    compareDocuments(document, cursorDocument, {
      order: {
        field: cursor.orderField,
        direction: cursor.orderDirection,
      },
    }) > 0
  );
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function getRetryDelayMs(attempt: number): number {
  return 5 * 2 ** attempt + Math.floor(Math.random() * 5);
}

function isRetryableConflict(error: unknown): boolean {
  return error instanceof RetryableMutationConflictError;
}

function requiresChangeCount(operation: CommitOperation): boolean {
  return (
    operation.type === "bump-table-versions" ||
    operation.type === "delete" ||
    operation.type === "insert" ||
    operation.type === "update"
  );
}

class MutationQueryBuilder implements QueryBuilder<RuntimeDocument> {
  private readonly database: MutationDatabase;
  private readonly state: QueryState;
  private readonly tableName: string;

  constructor(
    database: MutationDatabase,
    tableName: string,
    state: QueryState = createBaseQueryState()
  ) {
    this.database = database;
    this.tableName = tableName;
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
    return this.database.collectQuery(this.tableName, this.state, null);
  }

  async first(): Promise<RuntimeDocument | null> {
    return (await this.clone({ limit: 1 }).collect())[0] ?? null;
  }

  async unique(): Promise<RuntimeDocument> {
    const documents = await this.clone({ limit: 2 }).collect();
    if (documents.length === 0) {
      throw new NotFoundRuntimeError("Document not found");
    }

    if (documents.length > 1) {
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
    return (
      await this.database.collectQuery(
        this.tableName,
        { ...this.state, limit: undefined },
        null
      )
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
    const documents = await this.database.collectQuery(
      this.tableName,
      {
        ...this.state,
        limit: options.numItems + 1,
      },
      cursor
    );
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

  private clone(partial: Partial<QueryState>): MutationQueryBuilder {
    return new MutationQueryBuilder(this.database, this.tableName, {
      ...this.state,
      ...partial,
    });
  }
}

export class RetryableMutationConflictError extends Error {
  constructor(message = "Mutation commit conflicted with a concurrent write") {
    super(message);
    this.name = "RetryableMutationConflictError";
  }
}

export class MutationDatabase implements DatabaseWriter<RuntimeDocument> {
  private readonly database: SessionDatabase;
  private readonly functionName?: string;
  private readonly getContext: () => MutationCtx;
  private readonly pendingWrites = new Map<
    string,
    Map<string, PendingMutationWrite>
  >();
  private readonly rowReadRevisions = new Map<string, Map<string, number>>();
  private readonly rules?: Rules;
  private readonly schema: Schema;
  private readonly tableReadVersions = new Map<string, number>();

  constructor(options: {
    database: SessionDatabase;
    functionName?: string;
    getContext: () => MutationCtx;
    rules?: Rules;
    schema: Schema;
  }) {
    this.database = options.database;
    this.functionName = options.functionName;
    this.getContext = options.getContext;
    this.rules = options.rules;
    this.schema = options.schema;
  }

  async commit(): Promise<void> {
    const mutatedTables = this.getMutatedTables();
    if (mutatedTables.length === 0) {
      return;
    }

    await this.assertTableVersions(mutatedTables);

    const operations = this.buildCommitOperations(mutatedTables);
    if (operations.length === 0) {
      return;
    }

    const statements = operations.map((operation) =>
      bindStatement(this.database, operation.sql, operation.params)
    );

    try {
      const results = await this.database.batch(statements);
      this.validateCommitResults(operations, results);
    } catch (error) {
      if (isRetryableConflict(error)) {
        throw new RetryableMutationConflictError();
      }

      coerceDatabaseError(error, "Failed to commit mutation transaction");
    }
  }

  async get(tableName: string, id: string): Promise<RuntimeDocument | null> {
    assertKnownTable(this.schema, tableName);

    const pendingWrite = this.getPendingWrite(tableName, id);
    if (pendingWrite) {
      if (pendingWrite.type === "delete") {
        return null;
      }

      const document = this.requirePendingDocument(tableName, id, pendingWrite);
      return (await this.canRead(tableName, document)) ? document : null;
    }

    const read = await this.fetchTableVersionAndRows<StoredDocumentRow>(
      tableName,
      {
        sql: `SELECT _id, _data, _rev FROM ${tableName} WHERE _id = ? LIMIT 1`,
        params: [id],
      }
    );
    const existing = read.rows[0];
    if (!existing) {
      this.recordTableReadVersion(tableName, read.version);
      return null;
    }

    const versioned = deserializeVersionedRuntimeDocument(tableName, existing);
    this.recordRowRead(tableName, id, versioned.rev);
    return (await this.canRead(tableName, versioned.document))
      ? versioned.document
      : null;
  }

  query(tableName: string): QueryBuilder<RuntimeDocument> {
    assertKnownTable(this.schema, tableName);
    return new MutationQueryBuilder(this, tableName);
  }

  async insert(
    tableName: string,
    doc: Record<string, unknown>
  ): Promise<string> {
    const table = assertKnownTable(this.schema, tableName);
    const validated = this.validateInsert(table, doc);
    await assertCanInsert(this.rules, tableName, this.getContext(), validated);

    const id = generateId();
    this.setPendingWrite(tableName, id, {
      type: "insert",
      document: this.createRuntimeDocument(id, validated),
      serializedData: serialize(validated)._data,
    });

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

    this.setPendingWrite(
      tableName,
      id,
      this.createUpdatedWrite(existing.baseRev, existing.write, id, validated)
    );
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

    this.setPendingWrite(
      tableName,
      id,
      this.createUpdatedWrite(existing.baseRev, existing.write, id, validated)
    );
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

    if (existing.write?.type === "insert") {
      this.setPendingWrite(tableName, id, { type: "delete" });
      return;
    }

    this.setPendingWrite(tableName, id, {
      type: "delete",
      baseRev: existing.write?.baseRev ?? existing.baseRev,
    });
  }

  async collectQuery(
    tableName: string,
    state: QueryState,
    cursor: CursorPayload | null
  ): Promise<RuntimeDocument[]> {
    const shadowedBaseIds = this.getShadowedBaseIds(tableName);
    const baseDocuments = await this.collectBaseDocuments(
      tableName,
      state,
      cursor,
      shadowedBaseIds
    );
    if (state.limit === 0) {
      return [];
    }

    const documents = [...baseDocuments];

    for (const document of await this.getReadableOverlayDocuments(
      tableName,
      state,
      cursor
    )) {
      documents.push(document);
    }

    documents.sort((left, right) => compareDocuments(left, right, state));
    return state.limit === undefined
      ? documents
      : documents.slice(0, state.limit);
  }

  private async collectBaseDocuments(
    tableName: string,
    state: QueryState,
    cursor: CursorPayload | null,
    shadowedBaseIds: Set<string>
  ): Promise<RuntimeDocument[]> {
    const chunkSize = getMutationQueryChunkSize(state, shadowedBaseIds.size);
    if (chunkSize === 0) {
      this.recordTableReadVersion(
        tableName,
        await this.fetchTableVersion(tableName)
      );
      return [];
    }

    const documents: RuntimeDocument[] = [];
    let scanPosition: RuntimeScanPosition = { cursor, offset: 0 };
    const budget: ScanBudget = { scannedBytes: 0, scannedRows: 0 };

    while (true) {
      const hasScanCursor = scanPosition.cursor !== null;
      const read = await this.fetchTableVersionAndRows<StoredDocumentRow>(
        tableName,
        buildRuntimeSelectQuery(tableName, state, {
          cursor: scanPosition.cursor ?? cursor,
          limit: chunkSize,
          offset: hasScanCursor ? undefined : scanPosition.offset,
        })
      );
      const { rows } = read;
      if (rows.length === 0) {
        this.recordTableReadVersion(tableName, read.version);
        break;
      }

      this.recordTableReadVersion(tableName, read.version);
      scanPosition = getNextRuntimeScanPosition(
        tableName,
        state,
        scanPosition,
        rows
      );
      // The table version is recorded before row processing, so a mid-chunk
      // limit return cannot skip OCC tracking for rows already read.
      if (
        await this.appendReadableBaseDocuments(
          tableName,
          state,
          rows,
          shadowedBaseIds,
          documents,
          budget
        )
      ) {
        return documents;
      }

      if (rows.length < chunkSize) {
        break;
      }
    }

    return documents;
  }

  private async appendReadableBaseDocuments(
    tableName: string,
    state: QueryState,
    rows: readonly StoredDocumentRow[],
    shadowedBaseIds: ReadonlySet<string>,
    documents: RuntimeDocument[],
    budget: ScanBudget
  ): Promise<boolean> {
    for (const row of rows) {
      if (shadowedBaseIds.has(row._id)) {
        continue;
      }

      budget.scannedRows += 1;
      budget.scannedBytes += row._data.length;
      assertWithinScanBudget(budget.scannedRows, budget.scannedBytes);

      const document = deserializeRuntimeDocument(tableName, row);
      if (await this.canRead(tableName, document)) {
        documents.push(document);
      }

      if (state.limit !== undefined && documents.length >= state.limit) {
        return true;
      }
    }

    return false;
  }

  private async getReadableOverlayDocuments(
    tableName: string,
    state: QueryState,
    cursor: CursorPayload | null
  ): Promise<RuntimeDocument[]> {
    const writes = this.pendingWrites.get(tableName);
    if (!writes) {
      return [];
    }

    const documents: RuntimeDocument[] = [];
    let scannedBytes = 0;
    let scannedRows = 0;
    for (const write of writes.values()) {
      if (write.type === "delete" || !write.document) {
        continue;
      }

      scannedRows += 1;
      scannedBytes +=
        write.serializedData?.length ?? JSON.stringify(write.document).length;
      assertWithinScanBudget(scannedRows, scannedBytes);

      if (!matchesFilter(state.filter, write.document)) {
        continue;
      }

      if (cursor && !isAfterCursor(write.document, cursor)) {
        continue;
      }

      if (await this.canRead(tableName, write.document)) {
        insertLimitedDocument(documents, write.document, state);
      }
    }

    return documents;
  }

  private async fetchTableVersion(tableName: string): Promise<unknown> {
    const rows = await executeRowQuery<{ version: number }>(this.database, {
      sql: `SELECT version FROM ${TABLE_VERSION_TABLE_NAME} WHERE table_name = ? LIMIT 1`,
      params: [tableName],
    });
    return rows[0]?.version;
  }

  private buildCommitOperations(
    mutatedTables: readonly string[]
  ): CommitOperation[] {
    const guard = this.createCommitGuard();
    const operations: CommitOperation[] = [
      createGuardedTableVersionBumps(mutatedTables, guard),
    ];
    let expectedPreviousChanges = mutatedTables.length;

    for (const tableName of mutatedTables) {
      expectedPreviousChanges = this.appendWriteStatementsForTable(
        operations,
        tableName,
        expectedPreviousChanges
      );
    }

    return operations;
  }

  private createRuntimeDocument(
    id: string,
    document: Record<string, unknown>
  ): RuntimeDocument {
    return {
      _id: id,
      _createdAt: getCreatedMsFromId(id),
      ...document,
    };
  }

  private createUpdatedWrite(
    baseRev: number | undefined,
    existingWrite: PendingMutationWrite | undefined,
    id: string,
    document: Record<string, unknown>
  ): PendingMutationWrite {
    const runtimeDocument = this.createRuntimeDocument(id, document);
    const serializedData = serialize(document)._data;

    if (existingWrite?.type === "insert") {
      return { type: "insert", document: runtimeDocument, serializedData };
    }

    return {
      type: "update",
      baseRev: existingWrite?.baseRev ?? baseRev,
      document: runtimeDocument,
      serializedData,
    };
  }

  private canRead(
    tableName: string,
    document: RuntimeDocument
  ): Promise<boolean> {
    return canReadDocument(this.rules, tableName, this.getContext(), document);
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

  private getPendingWrite(
    tableName: string,
    id: string
  ): PendingMutationWrite | undefined {
    return this.pendingWrites.get(tableName)?.get(id);
  }

  private setPendingWrite(
    tableName: string,
    id: string,
    write: PendingMutationWrite
  ): void {
    const writes =
      this.pendingWrites.get(tableName) ??
      new Map<string, PendingMutationWrite>();
    writes.set(id, write);
    this.pendingWrites.set(tableName, writes);
  }

  private async getWritableDocument(
    tableName: string,
    id: string
  ): Promise<{
    readonly baseRev?: number;
    readonly document: RuntimeDocument;
    readonly write?: PendingMutationWrite;
  }> {
    const pendingWrite = this.getPendingWrite(tableName, id);
    if (pendingWrite) {
      if (pendingWrite.type === "delete") {
        throw new NotFoundRuntimeError(
          `Document "${id}" was not found in table "${tableName}"`
        );
      }

      return {
        baseRev: pendingWrite.baseRev,
        document: this.requirePendingDocument(tableName, id, pendingWrite),
        write: pendingWrite,
      };
    }

    const read = await this.fetchTableVersionAndRows<StoredDocumentRow>(
      tableName,
      {
        sql: `SELECT _id, _data, _rev FROM ${tableName} WHERE _id = ? LIMIT 1`,
        params: [id],
      }
    );
    const existing = read.rows[0];
    if (!existing) {
      this.recordTableReadVersion(tableName, read.version);
      throw new NotFoundRuntimeError(
        `Document "${id}" was not found in table "${tableName}"`
      );
    }

    const versioned = deserializeVersionedRuntimeDocument(tableName, existing);
    this.recordRowRead(tableName, id, versioned.rev);
    return { baseRev: versioned.rev, document: versioned.document };
  }

  private getShadowedBaseIds(tableName: string): Set<string> {
    const writes = this.pendingWrites.get(tableName);
    if (!writes) {
      return new Set();
    }

    const ids = new Set<string>();
    for (const [id, write] of writes) {
      if (write.type === "update" || write.type === "delete") {
        ids.add(id);
      }
    }

    return ids;
  }

  private getMutatedTables(): string[] {
    const tableNames: string[] = [];
    for (const [tableName, writes] of this.pendingWrites) {
      let hasEffect = false;
      for (const write of writes.values()) {
        if (hasCommittedEffect(write)) {
          hasEffect = true;
          break;
        }
      }

      if (hasEffect) {
        tableNames.push(tableName);
      }
    }

    return tableNames;
  }

  private createCommitGuard(): CommitGuard {
    const insertedIds = new Map<string, Set<string>>();
    const rowRevisions = new Map<string, Map<string, number>>();

    for (const [tableName, writes] of this.pendingWrites) {
      for (const [id, write] of writes) {
        if (!hasCommittedEffect(write)) {
          continue;
        }

        if (write.type === "insert") {
          const ids = insertedIds.get(tableName) ?? new Set<string>();
          ids.add(id);
          insertedIds.set(tableName, ids);
          continue;
        }

        if (write.baseRev !== undefined) {
          const reads =
            rowRevisions.get(tableName) ?? new Map<string, number>();
          reads.set(id, write.baseRev);
          rowRevisions.set(tableName, reads);
        }
      }
    }

    for (const [tableName, reads] of this.rowReadRevisions) {
      const guardedReads =
        rowRevisions.get(tableName) ?? new Map<string, number>();
      for (const [id, rev] of reads) {
        guardedReads.set(id, rev);
      }
      rowRevisions.set(tableName, guardedReads);
    }

    return {
      insertedIds,
      rowRevisions,
      tableVersions: this.tableReadVersions,
    };
  }

  private appendWriteStatementsForTable(
    operations: CommitOperation[],
    tableName: string,
    expectedPreviousChanges: number
  ): number {
    let previousChanges = expectedPreviousChanges;
    const writes = this.pendingWrites.get(tableName);
    if (!writes) {
      return previousChanges;
    }

    for (const [id, write] of writes) {
      if (!hasCommittedEffect(write)) {
        continue;
      }

      operations.push(
        this.createWriteOperation(tableName, id, write, previousChanges)
      );
      previousChanges = 1;
    }

    return previousChanges;
  }

  private createWriteOperation(
    tableName: string,
    id: string,
    write: PendingMutationWrite,
    expectedPreviousChanges: number
  ): CommitOperation {
    if (write.type === "insert") {
      return {
        type: "insert",
        sql: `INSERT INTO ${tableName} (_id, _data, _rev)
              SELECT ?, ?, 0 WHERE changes() = ?`,
        params: [id, write.serializedData ?? "", expectedPreviousChanges],
        expectedChanges: 1,
      };
    }

    if (write.type === "update") {
      return {
        type: "update",
        sql: `UPDATE ${tableName}
              SET _data = ?, _rev = _rev + 1
              WHERE _id = ? AND _rev = ? AND changes() = ?`,
        params: [
          write.serializedData ?? "",
          id,
          write.baseRev ?? -1,
          expectedPreviousChanges,
        ],
        expectedChanges: 1,
      };
    }

    return {
      type: "delete",
      sql: `DELETE FROM ${tableName}
            WHERE _id = ? AND _rev = ? AND changes() = ?`,
      params: [id, write.baseRev ?? -1, expectedPreviousChanges],
      expectedChanges: 1,
    };
  }

  private validateCommitResults(
    operations: readonly CommitOperation[],
    results: readonly D1Result[]
  ): void {
    if (results.length !== operations.length) {
      throw new InternalRuntimeError(
        "Mutation commit returned an unexpected number of D1 results"
      );
    }

    for (const [index, operation] of operations.entries()) {
      const result = results[index];
      if (!result?.success) {
        throw new InternalRuntimeError(
          "Mutation commit reported an unsuccessful D1 result"
        );
      }

      const changes = result.meta?.changes;
      if (requiresChangeCount(operation)) {
        if (changes === undefined) {
          throw new InternalRuntimeError(
            `Mutation commit operation "${operation.type}" did not report a D1 change count`
          );
        }

        if (changes !== operation.expectedChanges) {
          // The guarded bump uses one global AND predicate, so D1 should report
          // either zero changed rows or every mutated table-version row.
          if (operation.type === "bump-table-versions" && changes === 0) {
            throw new RetryableMutationConflictError();
          }

          throw new InternalRuntimeError(
            `Mutation commit operation "${operation.type}" did not apply after its guard passed`
          );
        }
      }
    }
  }

  private async assertTableVersions(
    tableNames: readonly string[]
  ): Promise<void> {
    const checkedTables = new Set([
      ...tableNames,
      ...this.tableReadVersions.keys(),
    ]);
    if (checkedTables.size === 0) {
      return;
    }

    const checkedTableNames = [...checkedTables];
    const placeholders = checkedTableNames.map(() => "?").join(", ");
    const rows = await executeRowQuery<{
      table_name: string;
      version: number;
    }>(this.database, {
      sql: `SELECT table_name, version FROM ${TABLE_VERSION_TABLE_NAME} WHERE table_name IN (${placeholders})`,
      params: checkedTableNames,
    });

    const versions = new Map(
      rows.map((row) => [row.table_name, row.version] as const)
    );

    for (const tableName of checkedTableNames) {
      const version = versions.get(tableName);
      if (typeof version !== "number") {
        throw new InternalRuntimeError(
          `Missing internal table version row for "${tableName}"`
        );
      }

      const readVersion = this.tableReadVersions.get(tableName);
      if (readVersion !== undefined && version !== readVersion) {
        throw new RetryableMutationConflictError();
      }
    }
  }

  private requirePendingDocument(
    tableName: string,
    id: string,
    write: PendingMutationWrite
  ): RuntimeDocument {
    if (!write.document) {
      throw new InternalRuntimeError(
        `Pending write for "${tableName}/${id}" is missing its document state`
      );
    }

    return write.document;
  }

  private async fetchTableVersionAndRows<TRow extends Record<string, unknown>>(
    tableName: string,
    query: {
      readonly params: readonly (string | number | null)[];
      readonly sql: string;
    }
  ): Promise<TableVersionReadResult<TRow>> {
    const versionSql = `SELECT version FROM ${TABLE_VERSION_TABLE_NAME} WHERE table_name = ? LIMIT 1`;
    const versionStatement = bindStatement(this.database, versionSql, [
      tableName,
    ]);
    const queryStatement = bindStatement(
      this.database,
      query.sql,
      query.params
    );
    const results = await withDatabaseErrorHandling(
      "Failed to run D1 mutation read",
      async () => this.database.batch([versionStatement, queryStatement])
    );
    const versionResult = results[0];
    const queryResult = results[1];
    if (!(versionResult && queryResult)) {
      throw new InternalRuntimeError(
        "D1 mutation read did not return all batch results"
      );
    }

    ensureSuccessfulD1Result(
      versionResult,
      `Failed to run D1 query "${versionSql}"`
    );
    ensureSuccessfulD1Result(
      queryResult,
      `Failed to run D1 query "${query.sql}"`
    );

    return {
      version: versionResult.results?.[0]?.version,
      rows: (queryResult?.results ?? []) as readonly TRow[],
    };
  }

  private recordTableReadVersion(tableName: string, version: unknown): void {
    if (typeof version !== "number") {
      throw new InternalRuntimeError(
        `Missing internal table version row for "${tableName}"`
      );
    }

    const existing = this.tableReadVersions.get(tableName);
    if (existing === undefined) {
      this.tableReadVersions.set(tableName, version);
      return;
    }

    if (existing !== version) {
      throw new RetryableMutationConflictError();
    }
  }

  private recordRowRead(tableName: string, id: string, rev: number): void {
    const reads =
      this.rowReadRevisions.get(tableName) ?? new Map<string, number>();
    const existing = reads.get(id);
    if (existing !== undefined && existing !== rev) {
      throw new RetryableMutationConflictError();
    }

    reads.set(id, rev);
    this.rowReadRevisions.set(tableName, reads);
  }
}

export function isD1DatabaseSession(
  database: RuntimeDatabase
): database is D1DatabaseSession {
  return "getBookmark" in database;
}

export function createMutationDatabaseSession(
  database: RuntimeDatabase
): SessionDatabase {
  if (isD1DatabaseSession(database)) {
    return database;
  }

  // D1 Sessions provide the strongest mutation read consistency. Runtimes
  // without sessions fall back to the raw binding and rely on OCC at commit.
  return database.withSession?.("first-primary") ?? database;
}

export async function withMutationRetry<TResult>(
  execute: () => Promise<TResult>,
  maxAttempts = 3,
  functionName?: string
): Promise<TResult> {
  if (maxAttempts < 1) {
    throw new InternalRuntimeError(
      "withMutationRetry requires at least one attempt"
    );
  }

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      return await execute();
    } catch (error) {
      if (
        !(error instanceof RetryableMutationConflictError) ||
        attempt === maxAttempts - 1
      ) {
        if (error instanceof RetryableMutationConflictError) {
          logRuntimeEvent("error", "mutation.retry_exhausted", {
            attempts: maxAttempts,
            functionName,
          });
          throw new ConflictRuntimeError(
            "Mutation conflict retry limit exceeded"
          );
        }

        throw error;
      }

      const delayMs = getRetryDelayMs(attempt);
      logRuntimeEvent("warn", "mutation.retry_scheduled", {
        attempt: attempt + 1,
        delayMs,
        functionName,
        maxAttempts,
      });
      await delay(delayMs);
    }
  }

  // Defensive sentinel for future retry-loop refactors; current branches return
  // on success or throw on failure.
  throw new InternalRuntimeError("Mutation retry loop exited unexpectedly");
}
