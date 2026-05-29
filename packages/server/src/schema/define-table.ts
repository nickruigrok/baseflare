import { SchemaError, type ValidatorShape } from "@baseflare/values";

import {
  assertFieldName,
  assertIdentifier,
  type TableDefBuilder,
  type TableIndex,
} from "./types";

function validateIndexFields(
  fields: ValidatorShape,
  indexFields: readonly string[]
): void {
  if (indexFields.length === 0) {
    throw new SchemaError("Index must include at least one field");
  }

  for (const field of indexFields) {
    assertFieldName(field);

    if (!(field in fields)) {
      throw new SchemaError(
        `Index field "${field}" is not defined on the table`
      );
    }
  }
}

function createTableDefBuilder<TFields extends ValidatorShape>(
  fields: TFields,
  indexes: readonly TableIndex[] = []
): TableDefBuilder<TFields> {
  return {
    fields,
    indexes,
    index(
      name: string,
      indexFields: readonly string[]
    ): TableDefBuilder<TFields> {
      assertIdentifier(name, "Index name");
      validateIndexFields(fields, indexFields);

      if (indexes.some((index) => index.name === name)) {
        throw new SchemaError(
          `Index "${name}" is already defined on this table`
        );
      }

      return createTableDefBuilder(fields, [
        ...indexes,
        { name, fields: [...indexFields] },
      ]);
    },
  };
}

export function defineTable<TFields extends ValidatorShape>(
  fields: TFields
): TableDefBuilder<TFields> {
  if (Object.keys(fields).length === 0) {
    throw new SchemaError("Table definitions must include at least one field");
  }

  for (const [fieldName, validator] of Object.entries(fields)) {
    assertFieldName(fieldName);

    if (
      !validator ||
      typeof validator !== "object" ||
      !("validate" in validator)
    ) {
      throw new SchemaError(
        `Field "${fieldName}" must use a Baseflare validator`
      );
    }
  }

  return createTableDefBuilder({ ...fields });
}
