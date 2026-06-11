import {
  type FilterObject,
  type FilterValue,
  normalizeFilterValue,
} from "../db/filters";
import type { QueryState } from "../db/query-builder";
import type { TableIndex } from "../schema/types";

export interface PartitionReadTarget {
  readonly fields: readonly string[];
  readonly partitionKey: string;
  readonly partitionValue: string;
  readonly tableName: string;
}

export function partitionTargetId(
  target: Pick<
    PartitionReadTarget,
    "partitionKey" | "partitionValue" | "tableName"
  >
): string {
  return JSON.stringify([
    target.tableName,
    target.partitionKey,
    target.partitionValue,
  ]);
}

export function serializePartitionValue(
  values: readonly FilterValue[]
): string {
  return JSON.stringify(values.map((value) => normalizeFilterValue(value)));
}

export function isFilterValue(value: unknown): value is FilterValue {
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

export function getPartitionIndex(table: {
  readonly indexes: readonly TableIndex[];
}): TableIndex | undefined {
  return table.indexes.find((index) => index.partition === true);
}

export function getPartitionReadTarget(
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
