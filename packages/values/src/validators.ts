import type {
  AnyValidator,
  Id,
  ObjectInput,
  ObjectOutput,
  Primitive,
  ValidatorDefinition,
  ValidatorKind,
  ValidatorShape,
} from "./types";
import { getCreatedAtFromId, isUuidV7 } from "./uuid";

type MinMaxSupportedKind =
  | "string"
  | "number"
  | "float64"
  | "int64"
  | "array"
  | "bytes"
  | "vector";

type FullValidatorDefinition<
  TKind extends ValidatorKind,
  TOptional extends boolean,
  THasDefault extends boolean,
> = ValidatorDefinition & {
  readonly hasDefault: THasDefault;
  readonly kind: TKind;
  readonly optional: TOptional;
};

type InputValue<TValidator extends AnyValidator> =
  TValidator extends Validator<
    infer TInput,
    unknown,
    ValidatorKind,
    boolean,
    boolean
  >
    ? TInput
    : never;

type OutputValue<TValidator extends AnyValidator> =
  TValidator extends Validator<
    unknown,
    infer TOutput,
    ValidatorKind,
    boolean,
    boolean
  >
    ? TOutput
    : never;

export interface Validator<
  TInput,
  TOutput,
  TKind extends ValidatorKind = ValidatorKind,
  TOptional extends boolean = false,
  THasDefault extends boolean = false,
> {
  default<TDefault extends Exclude<TOutput, undefined>>(
    defaultValue: TDefault
  ): Validator<TInput | undefined, TDefault, TKind, false, true>;
  readonly definition: FullValidatorDefinition<TKind, TOptional, THasDefault>;
  max(limit: number): Validator<TInput, TOutput, TKind, TOptional, THasDefault>;
  min(limit: number): Validator<TInput, TOutput, TKind, TOptional, THasDefault>;
  optional(): Validator<
    TInput | undefined,
    TOutput | undefined,
    TKind,
    true,
    THasDefault
  >;
  searchable(): Validator<TInput, TOutput, TKind, TOptional, THasDefault>;
  validate(value: unknown, path?: string): TOutput;
}

function formatPath(path: string): string {
  return path === "value" ? "Value" : path;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function createValidationError(path: string, message: string): Error {
  return new Error(`${formatPath(path)} ${message}`);
}

function withConstraint<TOutput>(
  validator: (value: unknown, path: string) => TOutput,
  _definition: ValidatorDefinition,
  kind: "min" | "max",
  limit: number
): (value: unknown, path: string) => TOutput {
  return (value, path) => {
    const result = validator(value, path);
    let size: number | undefined;

    if (typeof result === "number") {
      size = result;
    } else if (typeof result === "string" || Array.isArray(result)) {
      size = result.length;
    } else if (result instanceof Uint8Array) {
      size = result.byteLength;
    }

    if (size === undefined) {
      throw createValidationError(path, `does not support .${kind}()`);
    }

    if (kind === "min" && size < limit) {
      throw createValidationError(path, `must be at least ${limit}`);
    }

    if (kind === "max" && size > limit) {
      throw createValidationError(path, `must be at most ${limit}`);
    }

    return result;
  };
}

function assertSupportsMinMax(definition: ValidatorDefinition): void {
  const supportedKinds: MinMaxSupportedKind[] = [
    "string",
    "number",
    "float64",
    "int64",
    "array",
    "bytes",
    "vector",
  ];

  if (!supportedKinds.includes(definition.kind as MinMaxSupportedKind)) {
    throw new Error(
      `Validator kind "${definition.kind}" does not support min/max constraints`
    );
  }
}

function createValidatorApi<
  TInput,
  TOutput,
  TKind extends ValidatorKind,
  TOptional extends boolean,
  THasDefault extends boolean,
>(
  definition: FullValidatorDefinition<TKind, TOptional, THasDefault>,
  validator: (value: unknown, path: string) => TOutput
): Validator<TInput, TOutput, TKind, TOptional, THasDefault> {
  const api: Validator<TInput, TOutput, TKind, TOptional, THasDefault> = {
    definition,
    validate(value: unknown, path = "value"): TOutput {
      return validator(value, path);
    },
    optional() {
      return createValidatorApi(
        {
          ...api.definition,
          optional: true as const,
        },
        (value, path) => {
          if (value === undefined) {
            return (
              api.definition.hasDefault
                ? (api.validate(undefined, path) as TOutput)
                : undefined
            ) as TOutput | undefined;
          }

          return api.validate(value, path) as TOutput | undefined;
        }
      ) as Validator<
        TInput | undefined,
        TOutput | undefined,
        TKind,
        true,
        THasDefault
      >;
    },
    default<TDefault extends Exclude<TOutput, undefined>>(
      defaultValue: TDefault
    ) {
      return createValidatorApi(
        {
          ...api.definition,
          optional: false as const,
          hasDefault: true as const,
        },
        (value, path) => {
          if (value === undefined) {
            return defaultValue;
          }

          return api.validate(value, path) as unknown as TDefault;
        }
      ) as Validator<TInput | undefined, TDefault, TKind, false, true>;
    },
    searchable() {
      return createValidatorApi(
        {
          ...api.definition,
          searchable: true,
        },
        (value, path) => api.validate(value, path)
      ) as Validator<TInput, TOutput, TKind, TOptional, THasDefault>;
    },
    min(limit: number) {
      assertSupportsMinMax(api.definition);
      return createValidatorApi(
        {
          ...api.definition,
          min: limit,
        },
        withConstraint(
          (value, path) => api.validate(value, path),
          api.definition,
          "min",
          limit
        )
      ) as Validator<TInput, TOutput, TKind, TOptional, THasDefault>;
    },
    max(limit: number) {
      assertSupportsMinMax(api.definition);
      return createValidatorApi(
        {
          ...api.definition,
          max: limit,
        },
        withConstraint(
          (value, path) => api.validate(value, path),
          api.definition,
          "max",
          limit
        )
      ) as Validator<TInput, TOutput, TKind, TOptional, THasDefault>;
    },
  };

  return api;
}

function createValidator<TInput, TOutput, TKind extends ValidatorKind>(
  definition: Omit<
    ValidatorDefinition,
    "optional" | "hasDefault" | "searchable"
  > & {
    readonly kind: TKind;
    readonly searchable?: boolean;
  },
  validator: (value: unknown, path: string) => TOutput
): Validator<TInput, TOutput, TKind, false, false> {
  return createValidatorApi(
    {
      ...definition,
      optional: false as const,
      hasDefault: false as const,
      searchable: definition.searchable ?? false,
    },
    validator
  );
}

function stringValidator(): Validator<string, string, "string"> {
  return createValidator(
    { kind: "string", searchable: false },
    (value, path) => {
      if (typeof value !== "string") {
        throw createValidationError(path, "must be a string");
      }

      return value;
    }
  );
}

function numberValidator(
  kind: "number" | "float64" | "int64"
): Validator<number, number, typeof kind> {
  return createValidator({ kind, searchable: false }, (value, path) => {
    if (
      typeof value !== "number" ||
      Number.isNaN(value) ||
      !Number.isFinite(value)
    ) {
      throw createValidationError(path, "must be a finite number");
    }

    if (kind === "int64" && !Number.isSafeInteger(value)) {
      throw createValidationError(path, "must be a safe integer");
    }

    return value;
  });
}

function booleanValidator(): Validator<boolean, boolean, "boolean"> {
  return createValidator(
    { kind: "boolean", searchable: false },
    (value, path) => {
      if (typeof value !== "boolean") {
        throw createValidationError(path, "must be a boolean");
      }

      return value;
    }
  );
}

function bytesValidator(): Validator<Uint8Array, Uint8Array, "bytes"> {
  return createValidator(
    { kind: "bytes", searchable: false },
    (value, path) => {
      if (!(value instanceof Uint8Array)) {
        throw createValidationError(path, "must be a Uint8Array");
      }

      return value;
    }
  );
}

function nullValidator(): Validator<null, null, "null"> {
  return createValidator({ kind: "null", searchable: false }, (value, path) => {
    if (value !== null) {
      throw createValidationError(path, "must be null");
    }

    return null;
  });
}

function idValidator<TTableName extends string>(
  tableName: TTableName
): Validator<Id<TTableName>, Id<TTableName>, "id"> {
  return createValidator(
    { kind: "id", tableName, searchable: false },
    (value, path) => {
      if (typeof value !== "string") {
        throw createValidationError(path, "must be a string id");
      }

      if (!isUuidV7(value)) {
        throw createValidationError(path, "must be a valid UUIDv7 id");
      }

      getCreatedAtFromId(value);
      return value as Id<TTableName>;
    }
  );
}

function arrayValidator<TItemValidator extends AnyValidator>(
  item: TItemValidator
): Validator<
  InputValue<TItemValidator>[],
  OutputValue<TItemValidator>[],
  "array"
> {
  return createValidator(
    { kind: "array", item, searchable: false },
    (value, path) => {
      if (!Array.isArray(value)) {
        throw createValidationError(path, "must be an array");
      }

      return value.map((entry, index) =>
        item.validate(entry, `${path}[${index}]`)
      ) as OutputValue<TItemValidator>[];
    }
  );
}

function objectValidator<TShape extends ValidatorShape>(
  shape: TShape
): Validator<ObjectInput<TShape>, ObjectOutput<TShape>, "object"> {
  return createValidator(
    { kind: "object", shape, searchable: false },
    (value, path) => {
      if (!isPlainObject(value)) {
        throw createValidationError(path, "must be an object");
      }

      const allowedKeys = new Set(Object.keys(shape));
      for (const key of Object.keys(value)) {
        if (!allowedKeys.has(key)) {
          throw createValidationError(
            `${path}.${key}`,
            "is not allowed by the schema"
          );
        }
      }

      const result: Record<string, unknown> = {};
      for (const [key, validator] of Object.entries(shape)) {
        const validated = validator.validate(value[key], `${path}.${key}`);
        if (validated !== undefined) {
          result[key] = validated;
        }
      }

      return result as ObjectOutput<TShape>;
    }
  );
}

function recordValidator<TValueValidator extends AnyValidator>(
  valueValidator: TValueValidator
): Validator<
  Record<string, InputValue<TValueValidator>>,
  Record<string, OutputValue<TValueValidator>>,
  "record"
> {
  return createValidator(
    { kind: "record", recordValue: valueValidator, searchable: false },
    (value, path) => {
      if (!isPlainObject(value)) {
        throw createValidationError(path, "must be an object");
      }

      const result: Record<string, unknown> = {};
      for (const [key, entry] of Object.entries(value)) {
        result[key] = valueValidator.validate(entry, `${path}.${key}`);
      }

      return result as Record<string, OutputValue<TValueValidator>>;
    }
  );
}

function unionValidator<
  TMembers extends readonly [AnyValidator, ...AnyValidator[]],
>(
  members: TMembers
): Validator<
  InputValue<TMembers[number]>,
  OutputValue<TMembers[number]>,
  "union"
> {
  return createValidator(
    { kind: "union", members, searchable: false },
    (value, path) => {
      const errors: string[] = [];

      for (const member of members) {
        try {
          return member.validate(value, path) as OutputValue<TMembers[number]>;
        } catch (error) {
          errors.push(error instanceof Error ? error.message : String(error));
        }
      }

      throw createValidationError(
        path,
        `did not match any union member: ${errors.join("; ")}`
      );
    }
  );
}

function literalValidator<TValue extends Primitive>(
  literalValue: TValue
): Validator<TValue, TValue, "literal"> {
  return createValidator(
    { kind: "literal", value: literalValue, searchable: false },
    (value, path) => {
      if (value !== literalValue) {
        throw createValidationError(
          path,
          `must equal ${JSON.stringify(literalValue)}`
        );
      }

      return literalValue;
    }
  );
}

function enumValidator<TValues extends readonly [string, ...string[]]>(
  values: TValues
): Validator<TValues[number], TValues[number], "enum"> {
  return createValidator(
    { kind: "enum", values, searchable: false },
    (value, path) => {
      if (typeof value !== "string" || !values.includes(value)) {
        throw createValidationError(
          path,
          `must be one of ${values.join(", ")}`
        );
      }

      return value as TValues[number];
    }
  );
}

function vectorValidator(options: {
  dimensions: number;
}): Validator<number[], number[], "vector"> {
  return createValidator(
    { kind: "vector", dimensions: options.dimensions, searchable: false },
    (value, path) => {
      if (!Array.isArray(value)) {
        throw createValidationError(path, "must be an array of numbers");
      }

      if (value.length !== options.dimensions) {
        throw createValidationError(
          path,
          `must contain exactly ${options.dimensions} dimensions`
        );
      }

      for (const [index, item] of value.entries()) {
        if (
          typeof item !== "number" ||
          Number.isNaN(item) ||
          !Number.isFinite(item)
        ) {
          throw createValidationError(
            `${path}[${index}]`,
            "must be a finite number"
          );
        }
      }

      return [...value];
    }
  );
}

function anyValidator(): Validator<unknown, unknown, "any"> {
  return createValidator({ kind: "any", searchable: false }, (value) => value);
}

export const v = {
  string: stringValidator,
  number: () => numberValidator("number"),
  float64: () => numberValidator("float64"),
  int64: () => numberValidator("int64"),
  boolean: booleanValidator,
  bytes: bytesValidator,
  null: nullValidator,
  id: idValidator,
  array: arrayValidator,
  object: objectValidator,
  record: recordValidator,
  union: <TMembers extends readonly [AnyValidator, ...AnyValidator[]]>(
    ...members: TMembers
  ) => unionValidator(members),
  literal: literalValidator,
  enum: enumValidator,
  vector: vectorValidator,
  any: anyValidator,
  optional: <TValidator extends AnyValidator>(validator: TValidator) =>
    validator.optional(),
};

export type {
  AnyValidator,
  Id,
  ObjectInput,
  ObjectOutput,
  ValidatorDefinition,
  ValidatorShape,
} from "./types";
