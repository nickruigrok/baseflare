import type { Validator } from "./validators";

export type Primitive = string | number | boolean | null;

export type Id<TableName extends string = string> = string & {
  readonly __baseflareId: "BaseflareId";
  readonly __tableName: TableName;
};

export type ValidatorKind =
  | "string"
  | "number"
  | "float64"
  | "int64"
  | "boolean"
  | "bytes"
  | "null"
  | "id"
  | "array"
  | "object"
  | "record"
  | "union"
  | "literal"
  | "enum"
  | "vector"
  | "any";

export interface ValidatorDefinition {
  readonly dimensions?: number;
  readonly hasDefault: boolean;
  readonly item?: AnyValidator;
  readonly kind: ValidatorKind;
  readonly max?: number;
  readonly members?: readonly AnyValidator[];
  readonly min?: number;
  readonly optional: boolean;
  readonly recordValue?: AnyValidator;
  readonly searchable: boolean;
  readonly shape?: ValidatorShape;
  readonly tableName?: string;
  readonly value?: Primitive;
  readonly values?: readonly string[];
}

export type AnyValidator = Validator<
  unknown,
  unknown,
  ValidatorKind,
  boolean,
  boolean
>;

export type InputOf<TValidator extends AnyValidator> =
  TValidator extends Validator<
    infer TInput,
    unknown,
    ValidatorKind,
    boolean,
    boolean
  >
    ? TInput
    : never;

export type OutputOf<TValidator extends AnyValidator> =
  TValidator extends Validator<
    unknown,
    infer TOutput,
    ValidatorKind,
    boolean,
    boolean
  >
    ? TOutput
    : never;

export type Infer<TValidator extends AnyValidator> = OutputOf<TValidator>;

export type ValidatorShape = Record<string, AnyValidator>;

type Simplify<TValue> = {
  [TKey in keyof TValue]: TValue[TKey];
} & {};

type InputOptionalKeys<TShape extends ValidatorShape> = {
  [TKey in keyof TShape]: undefined extends InputOf<TShape[TKey]>
    ? TKey
    : never;
}[keyof TShape];

type OutputOptionalKeys<TShape extends ValidatorShape> = {
  [TKey in keyof TShape]: TShape[TKey] extends Validator<
    unknown,
    unknown,
    ValidatorKind,
    true,
    false
  >
    ? TKey
    : never;
}[keyof TShape];

export type ObjectInput<TShape extends ValidatorShape> = Simplify<
  {
    [TKey in Exclude<keyof TShape, InputOptionalKeys<TShape>>]: InputOf<
      TShape[TKey]
    >;
  } & {
    [TKey in InputOptionalKeys<TShape>]?: Exclude<
      InputOf<TShape[TKey]>,
      undefined
    >;
  }
>;

export type ObjectOutput<TShape extends ValidatorShape> = Simplify<
  {
    [TKey in Exclude<keyof TShape, OutputOptionalKeys<TShape>>]: OutputOf<
      TShape[TKey]
    >;
  } & {
    [TKey in OutputOptionalKeys<TShape>]?: Exclude<
      OutputOf<TShape[TKey]>,
      undefined
    >;
  }
>;
