export const ErrorCode = {
  Unauthorized: "UNAUTHORIZED",
  PermissionDenied: "PERMISSION_DENIED",
  NotFound: "NOT_FOUND",
  ValidationError: "VALIDATION_ERROR",
  SchemaError: "SCHEMA_ERROR",
  DeployError: "DEPLOY_ERROR",
  Conflict: "CONFLICT",
  DatabaseError: "DATABASE_ERROR",
  MalformedDocument: "MALFORMED_DOCUMENT",
  NotImplemented: "NOT_IMPLEMENTED",
  InternalError: "INTERNAL_ERROR",
} as const;

export type ErrorCode = (typeof ErrorCode)[keyof typeof ErrorCode];

/**
 * Application-level error thrown by developer function code. The structured
 * `data` payload propagates to the client. When `data` is a string and no
 * explicit `message` is given, the string doubles as the error message.
 */
export class BaseflareError<TData = undefined> extends Error {
  readonly data: TData;

  constructor(data: TData, message?: string) {
    super(message ?? (typeof data === "string" ? data : "BaseflareError"));
    this.name = "BaseflareError";
    this.data = data;
  }
}

/** Thrown when a value fails schema/argument/return validation. */
export class ValidationError extends Error {
  readonly code = ErrorCode.ValidationError;
  readonly path: string;

  constructor(path: string, message: string) {
    super(message);
    this.name = "ValidationError";
    this.path = path;
  }
}

/** Thrown for invalid schema, table, field, or index definitions. */
export class SchemaError extends Error {
  readonly code = ErrorCode.SchemaError;

  constructor(message: string) {
    super(message);
    this.name = "SchemaError";
  }
}
