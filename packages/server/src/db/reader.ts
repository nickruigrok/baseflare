import type { PaginationOptions, PaginationResult } from "@baseflare/values";

import type { FilterPredicate } from "./filters";

export interface QueryBuilder<TDocument = Record<string, unknown>> {
  collect(): Promise<TDocument[]>;
  count(): Promise<number>;
  filter(predicate: FilterPredicate): QueryBuilder<TDocument>;
  first(): Promise<TDocument | null>;
  limit(limit: number): QueryBuilder<TDocument>;
  order(direction: "asc" | "desc"): QueryBuilder<TDocument>;
  paginate(options: PaginationOptions): Promise<PaginationResult<TDocument>>;
  take(count: number): Promise<TDocument[]>;
  unique(): Promise<TDocument>;
}

export interface DatabaseReader<TDocument = Record<string, unknown>> {
  get(table: string, id: string): Promise<TDocument | null>;
  query(table: string): QueryBuilder<TDocument>;
}
