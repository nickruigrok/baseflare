import { ValidationError } from "./errors";
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
import { getCreatedMsFromId, isUuidV7 } from "./uuid";

type MinMaxSupportedKind = "string" | "number" | "array" | "bytes" | "vector";

const LENGTH_BOUNDED_KINDS = new Set<ValidatorKind>([
  "string",
  "array",
  "bytes",
  "vector",
]);

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

type NumberValidatorApi<
  TInput,
  TOutput,
  TOptional extends boolean,
  THasDefault extends boolean,
> = Validator<TInput, TOutput, "number", TOptional, THasDefault>;

export interface Validator<
  TInput,
  TOutput,
  TKind extends ValidatorKind = ValidatorKind,
  TOptional extends boolean = false,
  THasDefault extends boolean = false,
> {
  default(
    defaultValue: Exclude<TOutput, undefined>
  ): Validator<
    TInput | undefined,
    Exclude<TOutput, undefined>,
    TKind,
    false,
    true
  >;
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

export interface NumberValidator<
  TInput = number,
  TOutput = number,
  TOptional extends boolean = false,
  THasDefault extends boolean = false,
> extends Validator<TInput, TOutput, "number", TOptional, THasDefault> {
  default(
    defaultValue: Exclude<TOutput, undefined>
  ): NumberValidator<
    TInput | undefined,
    Exclude<TOutput, undefined>,
    false,
    true
  >;
  integer(): NumberValidator<TInput, TOutput, TOptional, THasDefault>;
  max(limit: number): NumberValidator<TInput, TOutput, TOptional, THasDefault>;
  min(limit: number): NumberValidator<TInput, TOutput, TOptional, THasDefault>;
  optional(): NumberValidator<
    TInput | undefined,
    TOutput | undefined,
    true,
    THasDefault
  >;
  searchable(): NumberValidator<TInput, TOutput, TOptional, THasDefault>;
}

function formatPath(path: string): string {
  return path === "value" ? "Value" : path;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function createValidationError(path: string, message: string): ValidationError {
  return new ValidationError(path, `${formatPath(path)} ${message}`);
}

function boundMessage(
  definition: ValidatorDefinition,
  kind: "min" | "max",
  limit: number
): string {
  const comparator = kind === "min" ? "at least" : "at most";
  if (LENGTH_BOUNDED_KINDS.has(definition.kind)) {
    return `must have length ${comparator} ${limit}`;
  }
  return `must be ${comparator} ${limit}`;
}

function withConstraint<TOutput>(
  validator: (value: unknown, path: string) => TOutput,
  definition: ValidatorDefinition,
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
      throw createValidationError(path, boundMessage(definition, kind, limit));
    }

    if (kind === "max" && size > limit) {
      throw createValidationError(path, boundMessage(definition, kind, limit));
    }

    return result;
  };
}

function withIntegerConstraint<TOutput>(
  validator: (value: unknown, path: string) => TOutput,
  definition: ValidatorDefinition
): (value: unknown, path: string) => TOutput {
  return (value, path) => {
    const result = validator(value, path);

    if (definition.kind !== "number" || typeof result !== "number") {
      throw createValidationError(path, "does not support .integer()");
    }

    if (!Number.isSafeInteger(result)) {
      throw createValidationError(path, "must be a safe integer");
    }

    return result;
  };
}

function assertSupportsMinMax(definition: ValidatorDefinition): void {
  const supportedKinds: MinMaxSupportedKind[] = [
    "string",
    "number",
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
    default(defaultValue: Exclude<TOutput, undefined>) {
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

          return api.validate(value, path) as Exclude<TOutput, undefined>;
        }
      ) as Validator<
        TInput | undefined,
        Exclude<TOutput, undefined>,
        TKind,
        false,
        true
      >;
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

function asNumberValidator<
  TInput,
  TOutput,
  TOptional extends boolean,
  THasDefault extends boolean,
>(
  validator: NumberValidatorApi<TInput, TOutput, TOptional, THasDefault>
): NumberValidator<TInput, TOutput, TOptional, THasDefault> {
  const numberApi = validator as NumberValidator<
    TInput,
    TOutput,
    TOptional,
    THasDefault
  >;
  const defaultValidator = validator.default.bind(validator);
  const maxValidator = validator.max.bind(validator);
  const minValidator = validator.min.bind(validator);
  const optionalValidator = validator.optional.bind(validator);
  const searchableValidator = validator.searchable.bind(validator);

  numberApi.default = (defaultValue) =>
    asNumberValidator(
      defaultValidator(defaultValue) as NumberValidatorApi<
        TInput | undefined,
        Exclude<TOutput, undefined>,
        false,
        true
      >
    );
  numberApi.integer = () =>
    asNumberValidator(
      createValidatorApi(
        {
          ...validator.definition,
          integer: true,
        },
        withIntegerConstraint(
          (value, path) => validator.validate(value, path),
          validator.definition
        )
      )
    );
  numberApi.max = (limit) =>
    asNumberValidator(
      maxValidator(limit) as NumberValidatorApi<
        TInput,
        TOutput,
        TOptional,
        THasDefault
      >
    );
  numberApi.min = (limit) =>
    asNumberValidator(
      minValidator(limit) as NumberValidatorApi<
        TInput,
        TOutput,
        TOptional,
        THasDefault
      >
    );
  numberApi.optional = () =>
    asNumberValidator(
      optionalValidator() as NumberValidatorApi<
        TInput | undefined,
        TOutput | undefined,
        true,
        THasDefault
      >
    );
  numberApi.searchable = () =>
    asNumberValidator(
      searchableValidator() as NumberValidatorApi<
        TInput,
        TOutput,
        TOptional,
        THasDefault
      >
    );

  return numberApi;
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

function numberValidator(): NumberValidator {
  return asNumberValidator(
    createValidator({ kind: "number", searchable: false }, (value, path) => {
      if (
        typeof value !== "number" ||
        Number.isNaN(value) ||
        !Number.isFinite(value)
      ) {
        throw createValidationError(path, "must be a finite number");
      }

      return value;
    })
  );
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

      getCreatedMsFromId(value);
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
        Object.defineProperty(result, key, {
          configurable: true,
          enumerable: true,
          value: valueValidator.validate(entry, `${path}.${key}`),
          writable: true,
        });
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

function optionalValidator<
  TInput,
  TOutput,
  TKind extends ValidatorKind,
  THasDefault extends boolean,
>(
  validator: Validator<TInput, TOutput, TKind, boolean, THasDefault>
): Validator<
  TInput | undefined,
  TOutput | undefined,
  TKind,
  true,
  THasDefault
> {
  return validator.optional();
}

/**
 * Validator builders for schema fields, function args, and return values —
 * e.g. `v.string().min(1)`, `v.id("users")`, `v.optional(v.number())`.
 * Validators run at write/call time; chain `.optional()`, `.default()`,
 * `.min()`/`.max()`, and `.searchable()` where supported.
 */
export const v = {
  string: stringValidator,
  number: numberValidator,
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
  optional: optionalValidator,
};

export type {
  AnyValidator,
  Id,
  ObjectInput,
  ObjectOutput,
  ValidatorDefinition,
  ValidatorShape,
} from "./types";
