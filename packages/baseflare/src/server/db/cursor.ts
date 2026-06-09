import { isUuidV7, ValidationError } from "baseflare/values";

import {
  type FilterValue,
  fieldExpression,
  normalizeFilterValue,
} from "./filters";

export type OrderDirection = "asc" | "desc";

export interface OrderSpec {
  readonly direction: OrderDirection;
  /** Normalized order field — `"_id"` for default / `_createdAt` ordering. */
  readonly field: string;
}

export interface CursorPayload {
  readonly id: string;
  readonly orderDirection: OrderDirection;
  readonly orderField: string;
  readonly v?: FilterValue;
}

interface CursorPredicate {
  readonly params: Array<string | number | null>;
  readonly sql: string;
}

function scalarOrderValue(value: unknown, field: string): FilterValue {
  if (value === undefined || value === null) {
    return null;
  }

  const type = typeof value;
  if (type === "string" || type === "number" || type === "boolean") {
    return value as FilterValue;
  }

  throw new ValidationError(
    field,
    `Cannot paginate ordered by "${field}": value must be a string, number, boolean, or null`
  );
}

function requireDocumentId(document: Record<string, unknown>): string {
  const id = document._id;
  if (typeof id !== "string") {
    throw new ValidationError(
      "_id",
      'Paginated query results must include a string "_id" field'
    );
  }

  return id;
}

export function encodeCursor(
  order: OrderSpec,
  document: Record<string, unknown>
): string {
  const id = requireDocumentId(document);
  const payload: CursorPayload =
    order.field === "_id"
      ? { orderField: "_id", orderDirection: order.direction, id }
      : {
          orderField: order.field,
          orderDirection: order.direction,
          id,
          v: scalarOrderValue(document[order.field], order.field),
        };

  return Buffer.from(JSON.stringify(payload), "utf8").toString("base64");
}

function isOrderDirection(value: unknown): value is OrderDirection {
  return value === "asc" || value === "desc";
}

function hasOwnKey(value: Record<string, unknown>, key: string): boolean {
  return Object.hasOwn(value, key);
}

function assertDecodedCursorValue(
  payload: Record<string, unknown>,
  order: OrderSpec
): void {
  const hasValue = hasOwnKey(payload, "v");

  if (order.field === "_id") {
    if (hasValue) {
      throw new ValidationError(
        "cursor.v",
        'Pagination cursors ordered by "_id" must not include "v"'
      );
    }
    return;
  }

  if (!hasValue) {
    throw new ValidationError(
      "cursor.v",
      `Pagination cursors ordered by "${order.field}" must include "v"`
    );
  }

  scalarOrderValue(payload.v, "cursor.v");
}

export function decodeCursor(cursor: string, order: OrderSpec): CursorPayload {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, "base64").toString("utf8"));
  } catch {
    throw new ValidationError("cursor", "Invalid pagination cursor");
  }

  if (
    typeof parsed !== "object" ||
    parsed === null ||
    Array.isArray(parsed) ||
    typeof (parsed as Record<string, unknown>).orderField !== "string" ||
    typeof (parsed as Record<string, unknown>).id !== "string" ||
    !isUuidV7((parsed as Record<string, unknown>).id as string) ||
    !isOrderDirection((parsed as Record<string, unknown>).orderDirection)
  ) {
    throw new ValidationError("cursor", "Invalid pagination cursor");
  }

  const payload = parsed as Record<string, unknown>;
  if (
    payload.orderField !== order.field ||
    payload.orderDirection !== order.direction
  ) {
    throw new ValidationError(
      "cursor",
      "Pagination cursor does not match the query ordering"
    );
  }

  assertDecodedCursorValue(payload, order);

  // Rebuild the payload so unknown fields from forged cursors are dropped.
  return order.field === "_id"
    ? {
        id: payload.id as string,
        orderDirection: order.direction,
        orderField: order.field,
      }
    : {
        id: payload.id as string,
        orderDirection: order.direction,
        orderField: order.field,
        v: scalarOrderValue(payload.v, "cursor.v"),
      };
}

export function buildCursorPredicate(
  order: OrderSpec,
  payload: CursorPayload
): CursorPredicate {
  if (order.field === "_id") {
    const operator = order.direction === "asc" ? ">" : "<";
    return { sql: `_id ${operator} ?`, params: [payload.id] };
  }

  const column = fieldExpression(order.field);
  const value = payload.v ?? null;

  if (order.direction === "asc") {
    if (value === null) {
      return {
        sql: `((${column} IS NULL AND _id > ?) OR ${column} IS NOT NULL)`,
        params: [payload.id],
      };
    }

    const normalized = normalizeFilterValue(value);
    return {
      sql: `(${column} > ? OR (${column} = ? AND _id > ?))`,
      params: [normalized, normalized, payload.id],
    };
  }

  if (value === null) {
    return {
      sql: `(${column} IS NULL AND _id < ?)`,
      params: [payload.id],
    };
  }

  const normalized = normalizeFilterValue(value);
  return {
    sql: `(${column} < ? OR (${column} = ? AND _id < ?) OR ${column} IS NULL)`,
    params: [normalized, normalized, payload.id],
  };
}
