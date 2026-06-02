import { maxIdForMs, minIdForMs, ValidationError } from "baseflare/values";

import { toStorageValue } from "./serialize";

export type FilterValue = string | number | boolean | null;

export type FilterOperator = "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in";

export type FieldFilter =
  | FilterValue
  | {
      readonly eq?: FilterValue;
      readonly gt?: FilterValue;
      readonly gte?: FilterValue;
      readonly in?: readonly FilterValue[];
      readonly lt?: FilterValue;
      readonly lte?: FilterValue;
      readonly neq?: FilterValue;
    };

export type LogicalFilterKey = "AND" | "OR" | "NOT";

export interface FilterObject {
  readonly AND?: readonly FilterObject[];
  readonly NOT?: FilterObject;
  readonly OR?: readonly FilterObject[];
  readonly [fieldName: string]:
    | FieldFilter
    | FilterObject
    | readonly FilterObject[]
    | undefined;
}

export interface CompiledFilter {
  readonly params: Array<string | number | null>;
  readonly sql: string;
}

const IDENTIFIER_PATTERN = /^[A-Za-z][A-Za-z0-9_]*$/;
const ID_FIELD = "_id";
const CREATED_AT_FIELD = "_createdAt";
const QUERYABLE_RESERVED_FIELDS = new Set([ID_FIELD, CREATED_AT_FIELD]);
const LOGICAL_FILTER_KEYS = new Set(["AND", "OR", "NOT"]);
const FILTER_OPERATORS = new Set(["eq", "neq", "gt", "gte", "lt", "lte", "in"]);
const MAX_IN_VALUES = 100;
const COMPARISON_OPERATOR_SQL: Record<"gt" | "gte" | "lt" | "lte", string> = {
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<=",
};

type ComparableJsonValue =
  | {
      readonly rank: 0;
      readonly value: null;
    }
  | {
      readonly rank: 1;
      readonly value: number;
    }
  | {
      readonly rank: 2;
      readonly value: string;
    };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function error(path: string, message: string): ValidationError {
  return new ValidationError(path, `${path} ${message}`);
}

function isFilterValue(value: unknown): value is FilterValue {
  const type = typeof value;
  return (
    value === null ||
    type === "string" ||
    type === "number" ||
    type === "boolean"
  );
}

function assertFilterValue(
  value: unknown,
  path: string
): asserts value is FilterValue {
  if (!isFilterValue(value)) {
    throw error(path, "must be a string, number, boolean, or null");
  }
}

function assertTimestamp(
  value: FilterValue,
  path: string
): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw error(path, "must be a non-negative millisecond timestamp number");
  }
}

export function assertQueryField(name: string): void {
  if (QUERYABLE_RESERVED_FIELDS.has(name)) {
    return;
  }

  if (!IDENTIFIER_PATTERN.test(name) || name.startsWith("_")) {
    throw error(
      `filter.${name}`,
      `must be "_id", "_createdAt", or start with a letter and contain only letters, numbers, and underscores`
    );
  }
}

export function normalizeFilterValue(
  value: FilterValue
): string | number | null {
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  return value;
}

function toComparableJsonValue(value: unknown): ComparableJsonValue {
  if (value === null || value === undefined) {
    return { rank: 0, value: null };
  }

  if (typeof value === "boolean") {
    return { rank: 1, value: value ? 1 : 0 };
  }

  if (typeof value === "number") {
    return { rank: 1, value };
  }

  if (typeof value === "string") {
    return { rank: 2, value };
  }

  const serialized = JSON.stringify(toStorageValue(value));
  return { rank: 2, value: serialized ?? String(value) };
}

export function compareSqliteJsonValues(left: unknown, right: unknown): number {
  const leftValue = toComparableJsonValue(left);
  const rightValue = toComparableJsonValue(right);

  if (leftValue.rank !== rightValue.rank) {
    return leftValue.rank < rightValue.rank ? -1 : 1;
  }

  if (leftValue.value === rightValue.value) {
    return 0;
  }

  return (leftValue.value as number | string) <
    (rightValue.value as number | string)
    ? -1
    : 1;
}

export function matchesSqliteJsonComparison(
  left: unknown,
  operator: "gt" | "gte" | "lt" | "lte",
  right: FilterValue
): boolean {
  if (left === null || left === undefined || right === null) {
    return false;
  }

  const comparison = compareSqliteJsonValues(left, right);

  switch (operator) {
    case "gt":
      return comparison > 0;
    case "gte":
      return comparison >= 0;
    case "lt":
      return comparison < 0;
    case "lte":
      return comparison <= 0;
    default:
      return false;
  }
}

/** SQL fragment that addresses a field for filtering, ordering, or row comparison. */
export function fieldExpression(name: string): string {
  if (name === ID_FIELD || name === CREATED_AT_FIELD) {
    return ID_FIELD;
  }

  return `json_extract(_data, '$.${name}')`;
}

function fieldTypeExpression(name: string): string {
  if (name === ID_FIELD) {
    return `${ID_FIELD} IS NOT NULL`;
  }

  return `json_type(_data, '$.${name}')`;
}

function combineCompiledFilters(
  filters: readonly CompiledFilter[],
  joiner: "AND" | "OR"
): CompiledFilter {
  return {
    sql: `(${filters.map((filter) => filter.sql).join(` ${joiner} `)})`,
    params: filters.flatMap((filter) => filter.params),
  };
}

function compileCreatedAtFilter(
  operator: FilterOperator,
  value: FilterValue,
  path: string
): CompiledFilter {
  if (operator === "eq" || operator === "neq" || operator === "in") {
    throw error(path, "does not support eq, neq, or in; use gt/gte/lt/lte");
  }

  assertTimestamp(value, path);

  switch (operator) {
    case "gte":
      return { sql: `${ID_FIELD} >= ?`, params: [minIdForMs(value)] };
    case "gt":
      return { sql: `${ID_FIELD} > ?`, params: [maxIdForMs(value)] };
    case "lt":
      return { sql: `${ID_FIELD} < ?`, params: [minIdForMs(value)] };
    case "lte":
      return { sql: `${ID_FIELD} <= ?`, params: [maxIdForMs(value)] };
    default:
      throw error(path, "does not support this operator");
  }
}

function compileNullEquality(
  fieldName: string,
  operator: "eq" | "neq"
): CompiledFilter {
  if (fieldName === ID_FIELD) {
    return {
      sql: `${ID_FIELD} IS ${operator === "eq" ? "" : "NOT "}NULL`,
      params: [],
    };
  }

  const jsonType = fieldTypeExpression(fieldName);
  const nullCheck =
    operator === "eq"
      ? `${jsonType} IS NULL OR ${jsonType} IS 'null'`
      : `${jsonType} IS NOT NULL AND ${jsonType} IS NOT 'null'`;

  return {
    sql: `(${nullCheck})`,
    params: [],
  };
}

function compileEqualityFilter(
  fieldName: string,
  operator: "eq" | "neq",
  value: FilterValue
): CompiledFilter {
  if (value === null) {
    return compileNullEquality(fieldName, operator);
  }

  const column = fieldExpression(fieldName);
  const sqlOperator = operator === "eq" ? "IS" : "IS NOT";
  return {
    sql: `${column} ${sqlOperator} ?`,
    params: [normalizeFilterValue(value)],
  };
}

function compileComparisonFilter(
  fieldName: string,
  operator: "gt" | "gte" | "lt" | "lte",
  value: FilterValue,
  path: string
): CompiledFilter {
  if (value === null) {
    throw error(path, "cannot compare against null");
  }

  const column = fieldExpression(fieldName);
  return {
    sql: `COALESCE(${column} ${COMPARISON_OPERATOR_SQL[operator]} ?, 0)`,
    params: [normalizeFilterValue(value)],
  };
}

function compileInFilter(
  fieldName: string,
  values: readonly FilterValue[],
  path: string
): CompiledFilter {
  if (values.length === 0) {
    throw error(path, "must not be empty");
  }

  if (values.length > MAX_IN_VALUES) {
    throw error(path, `must not contain more than ${MAX_IN_VALUES} values`);
  }

  const nonNullValues = values.filter((value) => value !== null);
  const includesNull = nonNullValues.length !== values.length;
  const clauses: CompiledFilter[] = [];

  if (includesNull) {
    clauses.push(compileNullEquality(fieldName, "eq"));
  }

  if (nonNullValues.length > 0) {
    const placeholders = nonNullValues.map(() => "?").join(", ");
    clauses.push({
      sql: `${fieldExpression(fieldName)} IN (${placeholders})`,
      params: nonNullValues.map((value) => normalizeFilterValue(value)),
    });
  }

  return clauses.length === 1
    ? (clauses[0] as CompiledFilter)
    : combineCompiledFilters(clauses, "OR");
}

function compileFieldOperator(
  fieldName: string,
  operator: FilterOperator,
  value: unknown,
  path: string
): CompiledFilter {
  if (operator === "in") {
    if (fieldName === CREATED_AT_FIELD) {
      throw error(path, "does not support in; use gt/gte/lt/lte");
    }

    if (!Array.isArray(value)) {
      throw error(path, "must be an array");
    }

    for (const [index, item] of value.entries()) {
      assertFilterValue(item, `${path}[${index}]`);
    }

    return compileInFilter(fieldName, value, path);
  }

  assertFilterValue(value, path);

  if (fieldName === CREATED_AT_FIELD) {
    return compileCreatedAtFilter(operator, value, path);
  }

  if (operator === "eq" || operator === "neq") {
    return compileEqualityFilter(fieldName, operator, value);
  }

  return compileComparisonFilter(fieldName, operator, value, path);
}

function compileFieldFilter(
  fieldName: string,
  fieldFilter: unknown,
  path: string
): CompiledFilter {
  assertQueryField(fieldName);

  if (!isPlainObject(fieldFilter)) {
    assertFilterValue(fieldFilter, path);
    if (fieldName === CREATED_AT_FIELD) {
      throw error(path, "does not support equality; use gt/gte/lt/lte");
    }
    return compileEqualityFilter(fieldName, "eq", fieldFilter);
  }

  const entries = Object.entries(fieldFilter);
  if (entries.length === 0) {
    throw error(path, "must include at least one operator");
  }

  const compiled = entries.map(([operator, value]) => {
    if (!FILTER_OPERATORS.has(operator)) {
      throw error(`${path}.${operator}`, "is not a supported filter operator");
    }

    return compileFieldOperator(
      fieldName,
      operator as FilterOperator,
      value,
      `${path}.${operator}`
    );
  });

  return compiled.length === 1
    ? (compiled[0] as CompiledFilter)
    : combineCompiledFilters(compiled, "AND");
}

function compileLogicalFilter(
  key: LogicalFilterKey,
  value: unknown,
  path: string
): CompiledFilter {
  if (key === "NOT") {
    if (!isPlainObject(value)) {
      throw error(path, "must be a filter object");
    }

    const compiled = compileFilterObject(value as FilterObject, path);
    return { sql: `(NOT (${compiled.sql}))`, params: compiled.params };
  }

  if (!Array.isArray(value) || value.length === 0) {
    throw error(path, "must be a non-empty array of filter objects");
  }

  const compiled = value.map((item, index) => {
    if (!isPlainObject(item)) {
      throw error(`${path}[${index}]`, "must be a filter object");
    }

    return compileFilterObject(item as FilterObject, `${path}[${index}]`);
  });

  return combineCompiledFilters(compiled, key);
}

function compileFilterObject(
  filter: FilterObject,
  path: string
): CompiledFilter {
  if (!isPlainObject(filter)) {
    throw error(path, "must be a filter object");
  }

  const entries = Object.entries(filter);
  if (entries.length === 0) {
    throw error(path, "must include at least one condition");
  }

  const compiled = entries.map(([key, value]) => {
    if (LOGICAL_FILTER_KEYS.has(key)) {
      return compileLogicalFilter(
        key as LogicalFilterKey,
        value,
        `${path}.${key}`
      );
    }

    return compileFieldFilter(key, value, `${path}.${key}`);
  });

  return compiled.length === 1
    ? (compiled[0] as CompiledFilter)
    : combineCompiledFilters(compiled, "AND");
}

export function combineFilters(
  left: FilterObject | undefined,
  right: FilterObject
): FilterObject {
  return left ? { AND: [left, right] } : right;
}

export function compileFilter(filter: FilterObject): CompiledFilter {
  return compileFilterObject(filter, "filter");
}

function getDocumentField(
  document: Record<string, unknown>,
  fieldName: string
): unknown {
  return document[fieldName];
}

function normalizeDocumentValue(value: unknown): string | number | null {
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  if (typeof value === "string" || typeof value === "number") {
    return value;
  }

  if (value === null || value === undefined) {
    return null;
  }

  const serialized = JSON.stringify(toStorageValue(value));
  return serialized ?? null;
}

function compareValues(
  left: unknown,
  operator: "gt" | "gte" | "lt" | "lte",
  right: FilterValue
): boolean {
  return matchesSqliteJsonComparison(left, operator, right);
}

function valuesEqual(left: unknown, right: FilterValue): boolean {
  if (right === null) {
    return left === null || left === undefined;
  }

  return normalizeDocumentValue(left) === normalizeFilterValue(right);
}

function valuesNotEqual(left: unknown, right: FilterValue): boolean {
  if (right === null) {
    return left !== null && left !== undefined;
  }

  return !valuesEqual(left, right);
}

function matchesCreatedAtFilter(
  value: unknown,
  operator: FilterOperator,
  expected: unknown,
  path: string
): boolean {
  if (operator === "eq" || operator === "neq" || operator === "in") {
    throw error(path, "does not support eq, neq, or in; use gt/gte/lt/lte");
  }

  assertFilterValue(expected, path);
  assertTimestamp(expected, path);

  return compareValues(value, operator, expected);
}

function matchesFieldOperator(
  document: Record<string, unknown>,
  fieldName: string,
  operator: FilterOperator,
  expected: unknown,
  path: string
): boolean {
  const value = getDocumentField(document, fieldName);

  if (fieldName === CREATED_AT_FIELD) {
    return matchesCreatedAtFilter(value, operator, expected, path);
  }

  if (operator === "in") {
    if (!Array.isArray(expected)) {
      throw error(path, "must be an array");
    }

    if (expected.length === 0) {
      throw error(path, "must not be empty");
    }

    if (expected.length > MAX_IN_VALUES) {
      throw error(path, `must not contain more than ${MAX_IN_VALUES} values`);
    }

    return expected.some((item, index) => {
      assertFilterValue(item, `${path}[${index}]`);
      return valuesEqual(value, item);
    });
  }

  assertFilterValue(expected, path);

  if (operator === "eq") {
    return valuesEqual(value, expected);
  }

  if (operator === "neq") {
    return valuesNotEqual(value, expected);
  }

  return compareValues(value, operator, expected);
}

function matchesFieldFilter(
  document: Record<string, unknown>,
  fieldName: string,
  fieldFilter: unknown,
  path: string
): boolean {
  assertQueryField(fieldName);

  if (!isPlainObject(fieldFilter)) {
    assertFilterValue(fieldFilter, path);
    if (fieldName === CREATED_AT_FIELD) {
      throw error(path, "does not support equality; use gt/gte/lt/lte");
    }

    return valuesEqual(getDocumentField(document, fieldName), fieldFilter);
  }

  const entries = Object.entries(fieldFilter);
  if (entries.length === 0) {
    throw error(path, "must include at least one operator");
  }

  return entries.every(([operator, value]) => {
    if (!FILTER_OPERATORS.has(operator)) {
      throw error(`${path}.${operator}`, "is not a supported filter operator");
    }

    return matchesFieldOperator(
      document,
      fieldName,
      operator as FilterOperator,
      value,
      `${path}.${operator}`
    );
  });
}

function matchesLogicalFilter(
  document: Record<string, unknown>,
  key: LogicalFilterKey,
  value: unknown,
  path: string
): boolean {
  if (key === "NOT") {
    if (!isPlainObject(value)) {
      throw error(path, "must be a filter object");
    }

    return !matchesFilterObject(document, value as FilterObject, path);
  }

  if (!Array.isArray(value) || value.length === 0) {
    throw error(path, "must be a non-empty array of filter objects");
  }

  if (key === "AND") {
    return value.every((item, index) => {
      if (!isPlainObject(item)) {
        throw error(`${path}[${index}]`, "must be a filter object");
      }

      return matchesFilterObject(
        document,
        item as FilterObject,
        `${path}[${index}]`
      );
    });
  }

  return value.some((item, index) => {
    if (!isPlainObject(item)) {
      throw error(`${path}[${index}]`, "must be a filter object");
    }

    return matchesFilterObject(
      document,
      item as FilterObject,
      `${path}[${index}]`
    );
  });
}

function matchesFilterObject(
  document: Record<string, unknown>,
  filter: FilterObject,
  path: string
): boolean {
  if (!isPlainObject(filter)) {
    throw error(path, "must be a filter object");
  }

  const entries = Object.entries(filter);
  if (entries.length === 0) {
    throw error(path, "must include at least one condition");
  }

  return entries.every(([key, value]) => {
    if (LOGICAL_FILTER_KEYS.has(key)) {
      return matchesLogicalFilter(
        document,
        key as LogicalFilterKey,
        value,
        `${path}.${key}`
      );
    }

    return matchesFieldFilter(document, key, value, `${path}.${key}`);
  });
}

export function matchesFilter(
  filter: FilterObject | undefined,
  document: Record<string, unknown>
): boolean {
  return filter ? matchesFilterObject(document, filter, "filter") : true;
}
