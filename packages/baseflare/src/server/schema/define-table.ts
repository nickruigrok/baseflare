import { SchemaError, type ValidatorShape } from "baseflare/values";

import {
  assertFieldName,
  assertIdentifier,
  type TableBuilder,
  type TableIndex,
  type TableIndexOptions,
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

function createTableBuilder<TFields extends ValidatorShape>(
  fields: TFields,
  indexes: readonly TableIndex[] = []
): TableBuilder<TFields> {
  return {
    fields,
    indexes,
    index(
      name: string,
      indexFields: readonly string[],
      options: TableIndexOptions = {}
    ): TableBuilder<TFields> {
      assertIdentifier(name, "Index name");
      validateIndexFields(fields, indexFields);

      if (indexes.some((index) => index.name === name)) {
        throw new SchemaError(
          `Index "${name}" is already defined on this table`
        );
      }

      if (
        options.partition === true &&
        indexes.some((index) => index.partition === true)
      ) {
        throw new SchemaError("Only one index per table can be partitioned");
      }

      return createTableBuilder(fields, [
        ...indexes,
        {
          name,
          fields: [...indexFields],
          ...(options.partition === undefined
            ? {}
            : { partition: options.partition }),
        },
      ]);
    },
  };
}

export function defineTable<TFields extends ValidatorShape>(
  fields: TFields
): TableBuilder<TFields> {
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

  return createTableBuilder({ ...fields });
}
