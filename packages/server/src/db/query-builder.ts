import type { PaginationOptions, PaginationResult } from "@baseflare/values";

import { buildFilterClause, type FilterPredicate } from "./filters";
import type { QueryBuilder } from "./reader";

type QueryOrder = "asc" | "desc";

interface QueryState {
  readonly filters: FilterPredicate;
  readonly limit?: number;
  readonly order: QueryOrder;
}

interface BuiltQuery {
  readonly params: readonly (string | number | null)[];
  readonly sql: string;
}

interface QueryExecutor<TDocument> {
  collect(query: BuiltQuery): Promise<TDocument[]>;
  count?(query: BuiltQuery): Promise<number>;
}

const IDENTIFIER_PATTERN = /^[A-Za-z][A-Za-z0-9_]*$/;
const WHERE_PREFIX_PATTERN = /^WHERE /;

function assertTableIdentifier(tableName: string): void {
  if (!IDENTIFIER_PATTERN.test(tableName) || tableName.startsWith("_")) {
    throw new Error(
      `Table name "${tableName}" must start with a letter and contain only letters, numbers, and underscores`
    );
  }
}

function createBaseState(): QueryState {
  return {
    filters: {},
    order: "asc",
  };
}

function buildCursorClause(
  order: QueryOrder,
  cursor: string | null
): {
  sql: string;
  params: readonly string[];
} {
  if (cursor === null) {
    return { sql: "", params: [] };
  }

  return {
    sql: `_id ${order === "asc" ? ">" : "<"} ?`,
    params: [cursor],
  };
}

function mergeWhereClauses(clauses: string[]): string {
  const filtered = clauses.filter(Boolean);
  if (filtered.length === 0) {
    return "";
  }

  return `WHERE ${filtered.join(" AND ")}`;
}

function getDocumentId(document: unknown): string {
  if (
    typeof document !== "object" ||
    document === null ||
    !("_id" in document)
  ) {
    throw new Error('Paginated query results must include an "_id" field');
  }

  const value = document._id;
  if (typeof value !== "string") {
    throw new Error('Document "_id" fields must be strings');
  }

  return value;
}

class BaseflareQueryBuilder<TDocument extends Record<string, unknown>>
  implements QueryBuilder<TDocument>
{
  private readonly tableName: string;
  private readonly state: QueryState;
  private readonly executor?: QueryExecutor<TDocument>;

  constructor(
    tableName: string,
    state: QueryState = createBaseState(),
    executor?: QueryExecutor<TDocument>
  ) {
    assertTableIdentifier(tableName);
    this.tableName = tableName;
    this.state = state;
    this.executor = executor;
  }

  filter(predicate: FilterPredicate): QueryBuilder<TDocument> {
    return this.clone({
      filters: {
        ...this.state.filters,
        ...predicate,
      },
    });
  }

  order(direction: QueryOrder): QueryBuilder<TDocument> {
    return this.clone({ order: direction });
  }

  limit(limit: number): QueryBuilder<TDocument> {
    if (!Number.isInteger(limit) || limit < 0) {
      throw new Error("Query limits must be a non-negative integer");
    }

    return this.clone({ limit });
  }

  collect(): Promise<TDocument[]> {
    return this.requireExecutor().collect(this.toSQL());
  }

  async first(): Promise<TDocument | null> {
    const results = await this.clone({ limit: 1 }).collect();
    return results[0] ?? null;
  }

  async unique(): Promise<TDocument> {
    const results = await this.clone({ limit: 2 }).collect();

    if (results.length !== 1) {
      throw new Error(
        `Expected exactly one document, received ${results.length}`
      );
    }

    const result = results[0];
    if (!result) {
      throw new Error("Expected a document but none was returned");
    }

    return result;
  }

  take(count: number): Promise<TDocument[]> {
    return this.limit(count).collect();
  }

  async count(): Promise<number> {
    const executor = this.requireExecutor();
    const query = this.toCountSQL();
    if (executor.count) {
      return executor.count(query);
    }

    const results = await executor.collect(query);
    return results.length;
  }

  async paginate(
    options: PaginationOptions
  ): Promise<PaginationResult<TDocument>> {
    if (!Number.isInteger(options.numItems) || options.numItems <= 0) {
      throw new Error("Pagination requires a positive integer numItems value");
    }

    const executor = this.requireExecutor();
    const pageBuilder = this.clone({ limit: options.numItems + 1 });
    const cursorQuery = pageBuilder.toSQL(options.cursor);
    const results = await executor.collect(cursorQuery);
    const page = results.slice(0, options.numItems);
    const isDone = results.length <= options.numItems;

    return {
      page,
      isDone,
      continueCursor: page.at(-1)
        ? getDocumentId(page.at(-1))
        : (options.cursor ?? ""),
    };
  }

  toSQL(cursor: string | null = null): BuiltQuery {
    const filters = buildFilterClause(this.state.filters);
    const cursorClause = buildCursorClause(this.state.order, cursor);
    const whereClause = mergeWhereClauses([
      filters.sql.replace(WHERE_PREFIX_PATTERN, ""),
      cursorClause.sql,
    ]);
    const params: Array<string | number | null> = [
      ...filters.params,
      ...cursorClause.params,
    ];
    let sql = `SELECT _id, _data FROM ${this.tableName}`;

    if (whereClause) {
      sql += ` ${whereClause}`;
    }

    sql += ` ORDER BY _id ${this.state.order.toUpperCase()}`;

    if (this.state.limit !== undefined) {
      sql += " LIMIT ?";
      params.push(this.state.limit);
    }

    return { sql, params };
  }

  toCountSQL(cursor: string | null = null): BuiltQuery {
    const filters = buildFilterClause(this.state.filters);
    const cursorClause = buildCursorClause(this.state.order, cursor);
    const whereClause = mergeWhereClauses([
      filters.sql.replace(WHERE_PREFIX_PATTERN, ""),
      cursorClause.sql,
    ]);
    const params: Array<string | number | null> = [
      ...filters.params,
      ...cursorClause.params,
    ];
    let sql = `SELECT COUNT(*) AS count FROM ${this.tableName}`;

    if (whereClause) {
      sql += ` ${whereClause}`;
    }

    return { sql, params };
  }

  private clone(
    partial: Partial<QueryState>
  ): BaseflareQueryBuilder<TDocument> {
    return new BaseflareQueryBuilder(
      this.tableName,
      {
        ...this.state,
        ...partial,
      },
      this.executor
    );
  }

  private requireExecutor(): QueryExecutor<TDocument> {
    if (!this.executor) {
      throw new Error("This QueryBuilder is not connected to an executor");
    }

    return this.executor;
  }
}

export function createQueryBuilder<TDocument extends Record<string, unknown>>(
  tableName: string,
  executor?: QueryExecutor<TDocument>
): QueryBuilder<TDocument> & {
  toSQL(cursor?: string | null): BuiltQuery;
  toCountSQL(cursor?: string | null): BuiltQuery;
} {
  return new BaseflareQueryBuilder(tableName, createBaseState(), executor);
}
