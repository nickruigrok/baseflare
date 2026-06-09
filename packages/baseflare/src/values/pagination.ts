import { v } from "./validators";

export interface PaginationOptions {
  cursor: string | null;
  numItems: number;
}

export interface PaginationResult<TValue> {
  continueCursor: string;
  isDone: boolean;
  page: TValue[];
}

/**
 * Reusable args validator for paginated queries: `{ numItems, cursor }`.
 * Pass the validated value straight to `ctx.db.query(...).paginate(...)`.
 */
export const paginationOptsValidator = v.object({
  numItems: v.number().integer().min(1),
  cursor: v.union(v.string(), v.null()).default(null),
});
