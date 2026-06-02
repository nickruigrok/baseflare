import {
  type PaginationOptions,
  type PaginationResult,
  ValidationError,
} from "baseflare/values";

import {
  buildCursorPredicate,
  type CursorPayload,
  decodeCursor,
  encodeCursor,
  type OrderDirection,
  type OrderSpec,
} from "./cursor";
import {
  assertQueryField,
  combineFilters,
  compileFilter,
  type FilterObject,
  fieldExpression,
} from "./filters";
import type { QueryBuilder } from "./reader";

export interface QueryState {
  readonly filter?: FilterObject;
  readonly limit?: number;
  readonly order: OrderSpec;
}

export interface BuiltQuery {
  readonly params: readonly (string | number | null)[];
  readonly sql: string;
}

export interface QueryExecutor<TDocument> {
  collect(query: BuiltQuery): Promise<TDocument[]>;
  count?(query: BuiltQuery): Promise<number>;
}

const IDENTIFIER_PATTERN = /^[A-Za-z][A-Za-z0-9_]*$/;

export function assertTableIdentifier(tableName: string): void {
  if (!IDENTIFIER_PATTERN.test(tableName) || tableName.startsWith("_")) {
    throw new ValidationError(
      tableName,
      `Table name "${tableName}" must start with a letter and contain only letters, numbers, and underscores`
    );
  }
}

function assertDirection(value: string): asserts value is OrderDirection {
  if (value !== "asc" && value !== "desc") {
    throw new ValidationError(
      "order",
      `Order direction must be "asc" or "desc", received "${value}"`
    );
  }
}

export function createBaseQueryState(): QueryState {
  return { order: { field: "_id", direction: "asc" } };
}

export function buildOrderClause(order: OrderSpec): string {
  const direction = order.direction.toUpperCase();
  if (order.field === "_id") {
    return `ORDER BY _id ${direction}`;
  }

  return `ORDER BY ${fieldExpression(order.field)} ${direction}, _id ${direction}`;
}

class BaseflareQueryBuilder<TDocument extends Record<string, unknown>>
  implements QueryBuilder<TDocument>
{
  private readonly tableName: string;
  private readonly state: QueryState;
  private readonly executor?: QueryExecutor<TDocument>;

  constructor(
    tableName: string,
    state: QueryState = createBaseQueryState(),
    executor?: QueryExecutor<TDocument>
  ) {
    assertTableIdentifier(tableName);
    this.tableName = tableName;
    this.state = state;
    this.executor = executor;
  }

  filter(filter: FilterObject): QueryBuilder<TDocument> {
    return this.clone({
      filter: combineFilters(this.state.filter, filter),
    });
  }

  order(direction: OrderDirection): QueryBuilder<TDocument>;
  order(field: string, direction: OrderDirection): QueryBuilder<TDocument>;
  order(
    fieldOrDirection: string,
    maybeDirection?: OrderDirection
  ): QueryBuilder<TDocument> {
    if (maybeDirection === undefined) {
      assertDirection(fieldOrDirection);
      return this.clone({
        order: { field: "_id", direction: fieldOrDirection },
      });
    }

    assertDirection(maybeDirection);
    const field =
      fieldOrDirection === "_id" || fieldOrDirection === "_createdAt"
        ? "_id"
        : fieldOrDirection;
    if (field !== "_id") {
      assertQueryField(field);
    }

    return this.clone({ order: { field, direction: maybeDirection } });
  }

  limit(limit: number): QueryBuilder<TDocument> {
    if (!Number.isInteger(limit) || limit < 0) {
      throw new ValidationError(
        "limit",
        "Query limits must be a non-negative integer"
      );
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
      throw new ValidationError(
        "unique",
        `Expected exactly one document, received ${results.length}`
      );
    }

    const result = results[0];
    if (!result) {
      throw new ValidationError(
        "unique",
        "Expected a document but none was returned"
      );
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
      throw new ValidationError(
        "numItems",
        "Pagination requires a positive integer numItems value"
      );
    }

    const executor = this.requireExecutor();
    const cursor = options.cursor
      ? decodeCursor(options.cursor, this.state.order)
      : null;
    const pageQuery = this.clone({ limit: options.numItems + 1 }).toSQL(cursor);
    const results = await executor.collect(pageQuery);
    const page = results.slice(0, options.numItems);
    const isDone = results.length <= options.numItems;
    const lastDocument = page.at(-1);

    return {
      page,
      isDone,
      continueCursor: lastDocument
        ? encodeCursor(this.state.order, lastDocument)
        : (options.cursor ?? ""),
    };
  }

  toSQL(cursor: CursorPayload | null = null): BuiltQuery {
    return buildSelectQuery(this.tableName, this.state, cursor);
  }

  toCountSQL(): BuiltQuery {
    return buildCountQuery(this.tableName, this.state);
  }

  private clone(
    partial: Partial<QueryState>
  ): BaseflareQueryBuilder<TDocument> {
    return new BaseflareQueryBuilder(
      this.tableName,
      { ...this.state, ...partial },
      this.executor
    );
  }

  private requireExecutor(): QueryExecutor<TDocument> {
    if (!this.executor) {
      throw new ValidationError(
        "executor",
        "This QueryBuilder is not connected to an executor"
      );
    }

    return this.executor;
  }
}

export function createQueryBuilder<TDocument extends Record<string, unknown>>(
  tableName: string,
  executor?: QueryExecutor<TDocument>
): QueryBuilder<TDocument> & {
  toSQL(cursor?: CursorPayload | null): BuiltQuery;
  toCountSQL(): BuiltQuery;
} {
  return new BaseflareQueryBuilder(tableName, createBaseQueryState(), executor);
}

export function buildSelectQuery(
  tableName: string,
  state: QueryState,
  cursor: CursorPayload | null = null
): BuiltQuery {
  assertTableIdentifier(tableName);
  const clauses: string[] = [];
  const params: Array<string | number | null> = [];

  if (state.filter) {
    const compiled = compileFilter(state.filter);
    clauses.push(compiled.sql);
    params.push(...compiled.params);
  }

  if (cursor) {
    const predicate = buildCursorPredicate(state.order, cursor);
    clauses.push(predicate.sql);
    params.push(...predicate.params);
  }

  let sql = `SELECT _id, _data FROM ${tableName}`;
  if (clauses.length > 0) {
    sql += ` WHERE ${clauses.join(" AND ")}`;
  }

  sql += ` ${buildOrderClause(state.order)}`;

  if (state.limit !== undefined) {
    sql += " LIMIT ?";
    params.push(state.limit);
  }

  return { sql, params };
}

export function buildCountQuery(
  tableName: string,
  state: QueryState
): BuiltQuery {
  assertTableIdentifier(tableName);
  const params: Array<string | number | null> = [];
  let sql = `SELECT COUNT(*) AS count FROM ${tableName}`;

  if (state.filter) {
    const compiled = compileFilter(state.filter);
    sql += ` WHERE ${compiled.sql}`;
    params.push(...compiled.params);
  }

  return { sql, params };
}
