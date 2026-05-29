import type { PaginationOptions, PaginationResult } from "baseflare/values";

import type { FilterObject } from "./filters";

export type QueryOrderDirection = "asc" | "desc";

export interface QueryBuilder<TDocument = Record<string, unknown>> {
  collect(): Promise<TDocument[]>;
  count(): Promise<number>;
  filter(filter: FilterObject): QueryBuilder<TDocument>;
  first(): Promise<TDocument | null>;
  limit(limit: number): QueryBuilder<TDocument>;
  order(direction: QueryOrderDirection): QueryBuilder<TDocument>;
  order(field: string, direction: QueryOrderDirection): QueryBuilder<TDocument>;
  paginate(options: PaginationOptions): Promise<PaginationResult<TDocument>>;
  take(count: number): Promise<TDocument[]>;
  unique(): Promise<TDocument>;
}

export interface DatabaseReader<TDocument = Record<string, unknown>> {
  get(table: string, id: string): Promise<TDocument | null>;
  query(table: string): QueryBuilder<TDocument>;
}
