import type { AnyValidator } from "@baseflare/values";

import {
  assertFieldName,
  assertIdentifier,
  type TableDefBuilder,
  type TableIndex,
} from "./types";

function validateIndexFields(
  fields: Record<string, AnyValidator>,
  indexFields: readonly string[]
): void {
  if (indexFields.length === 0) {
    throw new Error("Index must include at least one field");
  }

  for (const field of indexFields) {
    assertFieldName(field);

    if (!(field in fields)) {
      throw new Error(`Index field "${field}" is not defined on the table`);
    }
  }
}

function createTableDefBuilder(
  fields: Record<string, AnyValidator>,
  indexes: readonly TableIndex[] = []
): TableDefBuilder {
  return {
    fields,
    indexes,
    index(name: string, indexFields: readonly string[]): TableDefBuilder {
      assertIdentifier(name, "Index name");
      validateIndexFields(fields, indexFields);

      if (indexes.some((index) => index.name === name)) {
        throw new Error(`Index "${name}" is already defined on this table`);
      }

      return createTableDefBuilder(fields, [
        ...indexes,
        { name, fields: [...indexFields] },
      ]);
    },
  };
}

export function defineTable(
  fields: Record<string, AnyValidator>
): TableDefBuilder {
  if (Object.keys(fields).length === 0) {
    throw new Error("Table definitions must include at least one field");
  }

  for (const [fieldName, validator] of Object.entries(fields)) {
    assertFieldName(fieldName);

    if (
      !validator ||
      typeof validator !== "object" ||
      !("validate" in validator)
    ) {
      throw new Error(`Field "${fieldName}" must use a Baseflare validator`);
    }
  }

  return createTableDefBuilder({ ...fields });
}
