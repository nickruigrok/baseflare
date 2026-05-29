import { maxIdForMs, minIdForMs, ValidationError } from "@baseflare/values";

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
const COMPARISON_OPERATOR_SQL: Record<"gt" | "gte" | "lt" | "lte", string> = {
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<=",
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

/** SQL fragment that addresses a field for filtering, ordering, or row comparison. */
export function fieldExpression(name: string): string {
  if (name === ID_FIELD) {
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
