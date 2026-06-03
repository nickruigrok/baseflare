import { generateId, getCreatedMsFromId } from "baseflare/values";

import { type CursorPayload, decodeCursor, encodeCursor } from "../db/cursor";
import {
  assertQueryField,
  compareSqliteJsonValues,
  type FilterObject,
  type FilterValue,
  matchesFilter,
  normalizeFilterValue,
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
import {
  PARTITION_VERSION_TABLE_NAME,
  type Schema,
  TABLE_VERSION_TABLE_NAME,
  type TableIndex,
} from "../schema/types";

import {
  assertKnownTable,
  assertWithinScanBudget,
  bindStatement,
  buildRuntimeSelectQuery,
  COUNT_SCAN_BUDGET_MESSAGE,
  type CommitGuard,
  createEnsurePartitionVersionRows,
  createGuardedPartitionVersionBumps,
  createGuardedTableVersionBumps,
  DEFAULT_SCAN_BUDGET_MESSAGE,
  deserializeRuntimeDocument,
  deserializeVersionedRuntimeDocument,
  getNextRuntimeScanPosition,
  getRuntimeScanQueryOptions,
  type PartitionVersionKey,
  type PartitionVersionRead,
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
  ValidationRuntimeError,
  withDatabaseErrorHandling,
} from "./errors";
import { emitRuntimeMetric, logRuntimeEvent } from "./logging";
import {
  assertCanDelete,
  assertCanInsert,
  assertCanUpdate,
  assertReadRulesConfigured,
  canReadDocument,
} from "./permissions";
import type { D1DatabaseSession, D1Result, RuntimeDatabase } from "./types";

type SessionDatabase = Pick<RuntimeDatabase, "batch" | "prepare">;

interface PendingMutationWrite {
  readonly baseRev?: number;
  readonly document?: RuntimeDocument;
  readonly previousDocument?: RuntimeDocument;
  readonly serializedData?: string;
  readonly type: "delete" | "insert" | "update";
}

interface TableVersionReadResult<TRow extends Record<string, unknown>> {
  readonly partition?: PartitionReadTarget;
  readonly rows: readonly TRow[];
  readonly version: unknown;
}

interface PartitionReadTarget extends PartitionVersionKey {
  readonly fields: readonly string[];
}

interface OccConflictMetricEvent {
  readonly partitionAligned: boolean;
  readonly partitioned: boolean;
  readonly scope: "partition" | "row" | "table";
  readonly table: string;
}

interface ScanBudget {
  scannedBytes: number;
  scannedRows: number;
}

type CommitOperation =
  | {
      readonly params: readonly (string | number | null)[];
      readonly sql: string;
      readonly type: "ensure-partition-versions";
    }
  | {
      readonly params: readonly (string | number | null)[];
      readonly sql: string;
      readonly tableNames: readonly string[];
      readonly tableVersions: ReadonlyMap<string, number>;
      readonly type: "assert-table-versions";
    }
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
      readonly type: "bump-partition-versions";
    }
  | {
      readonly partitionVersions: ReadonlyMap<string, PartitionVersionRead>;
      readonly params: readonly (string | number | null)[];
      readonly sql: string;
      readonly type: "assert-partition-versions";
    }
  | {
      readonly expectedChanges: number;
      readonly params: readonly (string | number | null)[];
      readonly sql: string;
      readonly type: "delete" | "insert" | "update";
    };

const MUTATION_QUERY_CHUNK_SIZE = 256;
const D1_SESSION_REQUIRED_MESSAGE =
  "Baseflare runtime misconfiguration: APP_DB does not support D1 Sessions required for consistent mutations.";
const OCC_CONFLICT_RETRY_METRIC = "baseflare.runtime.occ.conflict_retries";
const OCC_RETRY_EXHAUSTION_METRIC = "baseflare.runtime.occ.retry_exhaustions";
const OCC_CONTENTION_WARNING_THRESHOLD = 10;
const OCC_CONTENTION_WARNING_WINDOW_MS = 60_000;
const OCC_CONTENTION_WARNING_DEDUP_MS = 600_000;
const occContentionWarningState = new Map<
  string,
  { count: number; lastWarnedAtMs: number; windowStartedAtMs: number }
>();
const missingTableVersionRowMessage = (tableName: string): string =>
  `Missing internal table version row for "${tableName}"; run applyRuntimeSchema before handling runtime traffic`;
const missingPartitionVersionRowMessage = (
  partition: PartitionVersionKey
): string =>
  `Missing internal partition version row for "${partition.tableName}/${partition.partitionKey}/${partition.partitionValue}"; run applyRuntimeSchema before handling runtime traffic`;
const missingPositivePartitionVersionRowMessage = (
  partition: PartitionVersionRead
): string =>
  `Partition version row for "${partition.tableName}/${partition.partitionKey}/${partition.partitionValue}" was present during the read phase (version ${partition.version}) but is now missing; this indicates data corruption or unexpected manual deletion`;

declare const __BASEFLARE_DEV_WARNINGS__: boolean | undefined;

export function resetOccContentionWarningStateForTest(): void {
  occContentionWarningState.clear();
}

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

  // Pending inserts are merged and sliced after base reads. They can displace
  // fetched base rows, but only update/delete writes shadow existing D1 rows.
  return Math.min(state.limit + shadowedCount, MUTATION_QUERY_CHUNK_SIZE);
}

function mergeFilters(
  left: FilterObject | undefined,
  right: FilterObject
): FilterObject {
  return left ? { AND: [left, right] } : right;
}

function partitionVersionId(key: PartitionVersionKey): string {
  return JSON.stringify([key.tableName, key.partitionKey, key.partitionValue]);
}

function serializePartitionValue(values: readonly FilterValue[]): string {
  return JSON.stringify(values.map((value) => normalizeFilterValue(value)));
}

function isFilterValue(value: unknown): value is FilterValue {
  const type = typeof value;
  return (
    value === null ||
    type === "boolean" ||
    type === "number" ||
    type === "string"
  );
}

function getEqualityValue(
  filter: FilterObject | undefined,
  fieldName: string
): FilterValue | undefined {
  if (!filter) {
    return undefined;
  }

  const fieldFilter = filter[fieldName];
  if (isFilterValue(fieldFilter)) {
    return fieldFilter;
  }

  if (
    fieldFilter &&
    typeof fieldFilter === "object" &&
    !Array.isArray(fieldFilter) &&
    "eq" in fieldFilter &&
    isFilterValue(fieldFilter.eq)
  ) {
    return fieldFilter.eq;
  }

  for (const nested of filter.AND ?? []) {
    const value = getEqualityValue(nested, fieldName);
    if (value !== undefined) {
      return value;
    }
  }

  return undefined;
}

function getPartitionIndex(table: {
  readonly indexes: readonly TableIndex[];
}): TableIndex | undefined {
  return table.indexes.find((index) => index.partition === true);
}

function getPartitionReadTarget(
  tableName: string,
  table: { readonly indexes: readonly TableIndex[] },
  state: QueryState
): PartitionReadTarget | undefined {
  const partitionIndex = getPartitionIndex(table);
  if (!partitionIndex) {
    return undefined;
  }

  const values: FilterValue[] = [];
  for (const field of partitionIndex.fields) {
    const value = getEqualityValue(state.filter, field);
    if (value === undefined) {
      return undefined;
    }
    values.push(value);
  }

  return {
    fields: partitionIndex.fields,
    partitionKey: partitionIndex.name,
    partitionValue: serializePartitionValue(values),
    tableName,
  };
}

function getDocumentPartition(
  tableName: string,
  table: { readonly indexes: readonly TableIndex[] },
  document: RuntimeDocument
): PartitionVersionKey | undefined {
  const partitionIndex = getPartitionIndex(table);
  if (!partitionIndex) {
    return undefined;
  }

  const values: FilterValue[] = [];
  for (const field of partitionIndex.fields) {
    const value = document[field];
    if (!isFilterValue(value)) {
      return undefined;
    }
    values.push(value);
  }

  return {
    partitionKey: partitionIndex.name,
    partitionValue: serializePartitionValue(values),
    tableName,
  };
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

function emitOccMetric(name: string, event: OccConflictMetricEvent): void {
  emitRuntimeMetric(name, 1, {
    partitionAligned: event.partitionAligned,
    partitioned: event.partitioned,
    scope: event.scope,
    table: event.table,
  });
}

function recordOccConflictRetryMetrics(
  events: readonly OccConflictMetricEvent[]
): void {
  for (const event of events) {
    emitOccMetric(OCC_CONFLICT_RETRY_METRIC, event);
    if (
      typeof __BASEFLARE_DEV_WARNINGS__ !== "undefined" &&
      __BASEFLARE_DEV_WARNINGS__
    ) {
      maybeWarnOccContention(event);
    }
  }
}

function recordOccRetryExhaustionMetrics(
  events: readonly OccConflictMetricEvent[]
): void {
  for (const event of events) {
    emitOccMetric(OCC_RETRY_EXHAUSTION_METRIC, event);
  }
}

function maybeWarnOccContention(event: OccConflictMetricEvent): void {
  if (event.scope !== "table" || event.partitionAligned) {
    return;
  }

  const now = Date.now();
  const state = occContentionWarningState.get(event.table);
  const currentState =
    state && now - state.windowStartedAtMs <= OCC_CONTENTION_WARNING_WINDOW_MS
      ? state
      : { count: 0, lastWarnedAtMs: 0, windowStartedAtMs: now };

  currentState.count += 1;
  occContentionWarningState.set(event.table, currentState);

  const canWarn =
    currentState.count >= OCC_CONTENTION_WARNING_THRESHOLD &&
    now - currentState.lastWarnedAtMs >= OCC_CONTENTION_WARNING_DEDUP_MS;
  if (!canWarn) {
    return;
  }

  currentState.lastWarnedAtMs = now;
  logRuntimeEvent("warn", "runtime.occ_contention", {
    message: event.partitioned
      ? `Table "${event.table}" has contention on non-partition-aligned reads; route hot reads through its partition index.`
      : `Table "${event.table}" has contention on table-level reads; add a partition index if writes cluster around one access axis.`,
    table: event.table,
  });
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

function requiresChangeCount(
  operation: CommitOperation
): operation is Extract<CommitOperation, { readonly expectedChanges: number }> {
  return (
    operation.type === "bump-partition-versions" ||
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
      throw new ValidationRuntimeError(
        `Expected exactly one document from "${this.tableName}", received ${documents.length}`
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
        null,
        COUNT_SCAN_BUDGET_MESSAGE
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
  readonly conflicts: readonly OccConflictMetricEvent[];

  constructor(
    message = "Mutation commit conflicted with a concurrent write",
    conflicts: readonly OccConflictMetricEvent[] = []
  ) {
    super(message);
    this.conflicts = conflicts;
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
  private readonly partitionReadVersions = new Map<
    string,
    PartitionVersionRead
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

    const conflictMetricEvents =
      this.createOccConflictMetricEvents(mutatedTables);
    const operations = this.buildCommitOperations(mutatedTables);
    const statements = operations.map((operation) =>
      bindStatement(this.database, operation.sql, operation.params)
    );

    try {
      const results = await this.database.batch(statements);
      this.validateCommitResults(operations, results);
    } catch (error) {
      if (isRetryableConflict(error)) {
        recordOccConflictRetryMetrics(conflictMetricEvents);
        throw new RetryableMutationConflictError(
          undefined,
          conflictMetricEvents
        );
      }

      // RuntimeError subclasses raised during result validation propagate
      // unchanged; unexpected D1/platform errors are coerced and sanitized.
      coerceDatabaseError(error, "Failed to commit mutation transaction");
    }
  }

  async get(tableName: string, id: string): Promise<RuntimeDocument | null> {
    assertKnownTable(this.schema, tableName);
    assertReadRulesConfigured(this.rules);

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
      this.createUpdatedWrite(
        existing.baseRev,
        existing.write,
        id,
        validated,
        existing.document
      )
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
      this.createUpdatedWrite(
        existing.baseRev,
        existing.write,
        id,
        validated,
        existing.document
      )
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
      previousDocument: existing.write?.previousDocument ?? existing.document,
    });
  }

  async collectQuery(
    tableName: string,
    state: QueryState,
    cursor: CursorPayload | null,
    scanBudgetMessage = DEFAULT_SCAN_BUDGET_MESSAGE
  ): Promise<RuntimeDocument[]> {
    assertReadRulesConfigured(this.rules);
    const budget: ScanBudget = { scannedBytes: 0, scannedRows: 0 };
    const shadowedBaseIds = this.getShadowedBaseIds(tableName);
    const baseDocuments = await this.collectBaseDocuments(
      tableName,
      state,
      cursor,
      shadowedBaseIds,
      budget,
      scanBudgetMessage
    );
    if (state.limit === 0) {
      return [];
    }

    const documents = [...baseDocuments];

    for (const document of await this.getReadableOverlayDocuments(
      tableName,
      state,
      cursor,
      budget,
      scanBudgetMessage
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
    shadowedBaseIds: Set<string>,
    budget: ScanBudget,
    scanBudgetMessage: string
  ): Promise<RuntimeDocument[]> {
    const table = assertKnownTable(this.schema, tableName);
    const partition = getPartitionReadTarget(tableName, table, state);
    const chunkSize = getMutationQueryChunkSize(state, shadowedBaseIds.size);
    if (chunkSize === 0) {
      const read = await this.fetchReadVersionAndRows<StoredDocumentRow>(
        tableName,
        partition,
        {
          sql: `SELECT _id, _data, _rev FROM ${tableName} LIMIT 0`,
          params: [],
        }
      );
      this.recordReadVersion(tableName, read);
      return [];
    }

    const documents: RuntimeDocument[] = [];
    let scanPosition: RuntimeScanPosition = { cursor, offset: 0 };

    while (true) {
      const scanOptions = getRuntimeScanQueryOptions(scanPosition, cursor);
      const read = await this.fetchReadVersionAndRows<StoredDocumentRow>(
        tableName,
        partition,
        buildRuntimeSelectQuery(tableName, state, {
          cursor: scanOptions.cursor,
          limit: chunkSize,
          offset: scanOptions.offset,
        })
      );
      const { rows } = read;
      if (rows.length === 0) {
        this.recordReadVersion(tableName, read);
        break;
      }

      this.recordReadVersion(tableName, read);
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
          budget,
          scanBudgetMessage
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
    budget: ScanBudget,
    scanBudgetMessage: string
  ): Promise<boolean> {
    for (const row of rows) {
      if (shadowedBaseIds.has(row._id)) {
        continue;
      }

      budget.scannedRows += 1;
      budget.scannedBytes += row._data.length;
      assertWithinScanBudget(
        budget.scannedRows,
        budget.scannedBytes,
        scanBudgetMessage
      );

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
    cursor: CursorPayload | null,
    budget: ScanBudget,
    scanBudgetMessage: string
  ): Promise<RuntimeDocument[]> {
    const writes = this.pendingWrites.get(tableName);
    if (!writes) {
      return [];
    }

    const documents: RuntimeDocument[] = [];
    for (const write of writes.values()) {
      if (write.type === "delete" || !write.document) {
        continue;
      }

      if (write.serializedData === undefined) {
        throw new InternalRuntimeError(
          "Pending mutation write is missing serialized data"
        );
      }

      budget.scannedRows += 1;
      budget.scannedBytes += write.serializedData.length;
      assertWithinScanBudget(
        budget.scannedRows,
        budget.scannedBytes,
        scanBudgetMessage
      );

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

  private buildCommitOperations(
    mutatedTables: readonly string[]
  ): CommitOperation[] {
    this.assertMutatedTablesHaveCommittedWrites(mutatedTables);

    const guard = this.createCommitGuard();
    const partitionBumpTargets = this.getPartitionBumpTargets(mutatedTables);
    const checkedTableNames = [
      ...new Set([...mutatedTables, ...this.tableReadVersions.keys()]),
    ];
    const operations: CommitOperation[] = [];
    if (partitionBumpTargets.length > 0) {
      operations.push(createEnsurePartitionVersionRows(partitionBumpTargets));
    }
    operations.push(
      this.createTableVersionAssertionOperation(checkedTableNames)
    );
    if (this.partitionReadVersions.size > 0) {
      operations.push(this.createPartitionVersionAssertionOperation());
    }
    operations.push(createGuardedTableVersionBumps(mutatedTables, guard));
    if (partitionBumpTargets.length > 0) {
      operations.push(
        createGuardedPartitionVersionBumps(
          partitionBumpTargets,
          guard,
          mutatedTables.length
        )
      );
    }
    // D1 batches run statements in order inside one SQLite transaction, and
    // changes() observes the immediately previous statement. The plural
    // version bump is the OCC gate; if it affects 0 rows, every following
    // document write is already a SQL-level no-op through this chain.
    let expectedPreviousChanges =
      partitionBumpTargets.length > 0
        ? partitionBumpTargets.length
        : mutatedTables.length;

    for (const tableName of mutatedTables) {
      expectedPreviousChanges = this.appendWriteStatementsForTable(
        operations,
        tableName,
        expectedPreviousChanges
      );
    }

    return operations;
  }

  private createOccConflictMetricEvents(
    mutatedTables: readonly string[]
  ): OccConflictMetricEvent[] {
    const tableNames = new Set<string>(mutatedTables);
    const partitionAlignedTables = new Set<string>();

    for (const tableName of this.tableReadVersions.keys()) {
      tableNames.add(tableName);
    }

    for (const partition of this.partitionReadVersions.values()) {
      tableNames.add(partition.tableName);
      partitionAlignedTables.add(partition.tableName);
    }

    return [...tableNames].sort().map((tableName) => {
      const table = assertKnownTable(this.schema, tableName);
      const partitioned = getPartitionIndex(table) !== undefined;
      let scope: OccConflictMetricEvent["scope"] = "row";
      if (this.tableReadVersions.has(tableName)) {
        scope = "table";
      } else if (partitionAlignedTables.has(tableName)) {
        scope = "partition";
      }

      return {
        partitionAligned:
          partitionAlignedTables.has(tableName) &&
          !this.tableReadVersions.has(tableName),
        partitioned,
        scope,
        table: tableName,
      };
    });
  }

  private getPartitionBumpTargets(
    mutatedTables: readonly string[]
  ): PartitionVersionKey[] {
    const targets = new Map<string, PartitionVersionKey>();

    for (const tableName of mutatedTables) {
      const table = assertKnownTable(this.schema, tableName);
      const writes = this.pendingWrites.get(tableName);
      if (!(writes && getPartitionIndex(table))) {
        continue;
      }

      for (const write of writes.values()) {
        if (!hasCommittedEffect(write)) {
          continue;
        }

        for (const partition of this.getWritePartitions(
          tableName,
          table,
          write
        )) {
          targets.set(partitionVersionId(partition), partition);
        }
      }
    }

    return [...targets.values()].sort((left, right) =>
      partitionVersionId(left).localeCompare(partitionVersionId(right))
    );
  }

  private getWritePartitions(
    tableName: string,
    table: { readonly indexes: readonly TableIndex[] },
    write: PendingMutationWrite
  ): PartitionVersionKey[] {
    const partitions = new Map<string, PartitionVersionKey>();
    const addPartition = (document: RuntimeDocument | undefined): void => {
      if (!document) {
        return;
      }

      const partition = getDocumentPartition(tableName, table, document);
      if (partition) {
        partitions.set(partitionVersionId(partition), partition);
      }
    };

    if (write.type === "insert") {
      addPartition(write.document);
    } else if (write.type === "update") {
      addPartition(write.previousDocument);
      addPartition(write.document);
    } else {
      addPartition(write.previousDocument);
    }

    return [...partitions.values()];
  }

  private assertMutatedTablesHaveCommittedWrites(
    mutatedTables: readonly string[]
  ): void {
    for (const tableName of mutatedTables) {
      const writes = this.pendingWrites.get(tableName);
      if (!(writes && [...writes.values()].some(hasCommittedEffect))) {
        throw new InternalRuntimeError(
          `Mutated table "${tableName}" has no committed writes`
        );
      }
    }
  }

  private createTableVersionAssertionOperation(
    tableNames: readonly string[]
  ): CommitOperation {
    const placeholders = tableNames.map(() => "?").join(", ");
    return {
      type: "assert-table-versions",
      sql: `SELECT table_name, version FROM ${TABLE_VERSION_TABLE_NAME} WHERE table_name IN (${placeholders})`,
      params: tableNames,
      tableNames,
      tableVersions: this.tableReadVersions,
    };
  }

  private createPartitionVersionAssertionOperation(): CommitOperation {
    const partitions = [...this.partitionReadVersions.values()].sort(
      (left, right) =>
        partitionVersionId(left).localeCompare(partitionVersionId(right))
    );
    const conditions = partitions
      .map(
        () => "(table_name = ? AND partition_key = ? AND partition_value = ?)"
      )
      .join(" OR ");

    return {
      type: "assert-partition-versions",
      sql: `SELECT table_name, partition_key, partition_value, version FROM ${PARTITION_VERSION_TABLE_NAME} WHERE ${conditions}`,
      params: partitions.flatMap((partition) => [
        partition.tableName,
        partition.partitionKey,
        partition.partitionValue,
      ]),
      partitionVersions: this.partitionReadVersions,
    };
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
    document: Record<string, unknown>,
    previousDocument: RuntimeDocument
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
      previousDocument: existingWrite?.previousDocument ?? previousDocument,
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

    tableNames.sort();
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
      partitionVersions: [...this.partitionReadVersions.values()],
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
      throw new InternalRuntimeError(
        `Mutated table "${tableName}" has no committed writes`
      );
    }

    let appendedWrite = false;
    for (const [id, write] of writes) {
      if (!hasCommittedEffect(write)) {
        continue;
      }

      appendedWrite = true;
      operations.push(
        this.createWriteOperation(tableName, id, write, previousChanges)
      );
      previousChanges = 1;
    }

    // getMutatedTables() only returns tables with committed-effect writes. If
    // that invariant changes, the changes() chain must fail closed.
    if (!appendedWrite) {
      throw new InternalRuntimeError(
        `Mutated table "${tableName}" has no committed writes`
      );
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

      if (operation.type === "assert-table-versions") {
        // This SELECT is diagnostic metadata validation after D1 resolves the
        // batch. Atomicity comes from the guarded version bump and changes()
        // chain below, not from post-batch JavaScript validation.
        this.validateTableVersionAssertion(operation, result);
        continue;
      }

      if (operation.type === "assert-partition-versions") {
        this.validatePartitionVersionAssertion(operation, result);
        continue;
      }

      if (requiresChangeCount(operation)) {
        this.validateCommitChangeCount(operation, result.meta?.changes);
      }
    }
  }

  private validateCommitChangeCount(
    operation: Extract<CommitOperation, { expectedChanges: number }>,
    changes: number | undefined
  ): void {
    if (changes === undefined) {
      throw new InternalRuntimeError(
        `Mutation commit operation "${operation.type}" did not report a D1 change count`
      );
    }

    if (changes === operation.expectedChanges) {
      return;
    }

    // Document writes are SQL-gated by changes(), so an under-applied plural
    // bump means later writes were already no-ops inside the D1 batch. The
    // global SQL guard should be binary today, but any under-application is
    // still safest to retry.
    if (
      (operation.type === "bump-table-versions" ||
        operation.type === "bump-partition-versions") &&
      changes < operation.expectedChanges
    ) {
      throw new RetryableMutationConflictError();
    }

    throw new InternalRuntimeError(
      `Mutation commit operation "${operation.type}" applied ${changes} rows but expected ${operation.expectedChanges}`
    );
  }

  private validateTableVersionAssertion(
    operation: Extract<CommitOperation, { type: "assert-table-versions" }>,
    result: D1Result
  ): void {
    const versions = new Map(
      (result.results ?? []).flatMap((row) =>
        typeof row.table_name === "string" && typeof row.version === "number"
          ? ([[row.table_name, row.version]] as const)
          : []
      )
    );

    for (const tableName of operation.tableNames) {
      const version = versions.get(tableName);
      if (typeof version !== "number") {
        throw new InternalRuntimeError(
          missingTableVersionRowMessage(tableName)
        );
      }

      const readVersion = operation.tableVersions.get(tableName);
      if (readVersion !== undefined && version !== readVersion) {
        throw new RetryableMutationConflictError();
      }
    }
  }

  private validatePartitionVersionAssertion(
    operation: Extract<CommitOperation, { type: "assert-partition-versions" }>,
    result: D1Result
  ): void {
    const versions = new Map(
      (result.results ?? []).flatMap((row) => {
        if (
          typeof row.table_name !== "string" ||
          typeof row.partition_key !== "string" ||
          typeof row.partition_value !== "string" ||
          typeof row.version !== "number"
        ) {
          return [];
        }

        return [
          [
            partitionVersionId({
              tableName: row.table_name,
              partitionKey: row.partition_key,
              partitionValue: row.partition_value,
            }),
            row.version,
          ] as const,
        ];
      })
    );

    for (const partition of operation.partitionVersions.values()) {
      const version = versions.get(partitionVersionId(partition));
      if (typeof version !== "number") {
        if (partition.version === 0) {
          continue;
        }

        throw new InternalRuntimeError(
          missingPositivePartitionVersionRowMessage(partition)
        );
      }

      if (version !== partition.version) {
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
    // Each mutation read batches the table-version row with the data read so
    // OCC records the version that belongs to that specific result set.
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

  private async fetchReadVersionAndRows<TRow extends Record<string, unknown>>(
    tableName: string,
    partition: PartitionReadTarget | undefined,
    query: {
      readonly params: readonly (string | number | null)[];
      readonly sql: string;
    }
  ): Promise<TableVersionReadResult<TRow>> {
    if (!partition) {
      return this.fetchTableVersionAndRows(tableName, query);
    }

    const versionSql = `SELECT version FROM ${PARTITION_VERSION_TABLE_NAME} WHERE table_name = ? AND partition_key = ? AND partition_value = ? LIMIT 1`;
    const partitionParams = [
      partition.tableName,
      partition.partitionKey,
      partition.partitionValue,
    ];
    const results = await withDatabaseErrorHandling(
      "Failed to run D1 mutation partition read",
      async () =>
        this.database.batch([
          bindStatement(this.database, versionSql, partitionParams),
          bindStatement(this.database, query.sql, query.params),
        ])
    );
    const versionResult = results[0];
    const queryResult = results[1];
    if (!(versionResult && queryResult)) {
      throw new InternalRuntimeError(
        "D1 mutation partition read did not return all batch results"
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
      partition,
      version:
        versionResult.results?.[0] === undefined
          ? 0
          : versionResult.results[0].version,
      rows: (queryResult.results ?? []) as readonly TRow[],
    };
  }

  private recordReadVersion(
    tableName: string,
    read: TableVersionReadResult<Record<string, unknown>>
  ): void {
    if (read.partition) {
      this.recordPartitionReadVersion(read.partition, read.version);
      return;
    }

    this.recordTableReadVersion(tableName, read.version);
  }

  private recordTableReadVersion(tableName: string, version: unknown): void {
    if (typeof version !== "number") {
      throw new InternalRuntimeError(missingTableVersionRowMessage(tableName));
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

  private recordPartitionReadVersion(
    partition: PartitionVersionKey,
    version: unknown
  ): void {
    if (typeof version !== "number") {
      throw new InternalRuntimeError(
        missingPartitionVersionRowMessage(partition)
      );
    }

    const key = partitionVersionId(partition);
    const existing = this.partitionReadVersions.get(key);
    if (existing === undefined) {
      this.partitionReadVersions.set(key, { ...partition, version });
      return;
    }

    if (existing.version !== version) {
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

  if (!database.withSession) {
    logRuntimeEvent("error", "runtime.d1_session_required");
    throw new InternalRuntimeError(D1_SESSION_REQUIRED_MESSAGE);
  }

  return database.withSession("first-primary");
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
          recordOccRetryExhaustionMetrics(error.conflicts);
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
        remainingAttempts: maxAttempts - attempt - 1,
      });
      await delay(delayMs);
    }
  }

  // Defensive sentinel for future retry-loop refactors; current branches return
  // on success or throw on failure.
  throw new InternalRuntimeError("Mutation retry loop exited unexpectedly");
}
