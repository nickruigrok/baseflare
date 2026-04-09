export type FilterValue = string | number | boolean | null;

export type FilterPredicate = Record<string, FilterValue>;

const IDENTIFIER_PATTERN = /^[A-Za-z][A-Za-z0-9_]*$/;

export function assertFieldIdentifier(fieldName: string): void {
  if (!IDENTIFIER_PATTERN.test(fieldName) || fieldName.startsWith("_")) {
    throw new Error(
      `Filter field "${fieldName}" must start with a letter and contain only letters, numbers, and underscores`
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

export function buildFilterClause(predicate: FilterPredicate): {
  sql: string;
  params: Array<string | number | null>;
} {
  const entries = Object.entries(predicate);
  if (entries.length === 0) {
    return { sql: "", params: [] };
  }

  const fragments: string[] = [];
  const params: Array<string | number | null> = [];

  for (const [field, value] of entries) {
    assertFieldIdentifier(field);
    fragments.push(`json_extract(_data, '$.${field}') = ?`);
    params.push(normalizeFilterValue(value));
  }

  return {
    sql: `WHERE ${fragments.join(" AND ")}`,
    params,
  };
}
