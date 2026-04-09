export const ErrorCode = {
  Unauthorized: "UNAUTHORIZED",
  PermissionDenied: "PERMISSION_DENIED",
  NotFound: "NOT_FOUND",
  ValidationError: "VALIDATION_ERROR",
  SchemaError: "SCHEMA_ERROR",
  DeployError: "DEPLOY_ERROR",
  InternalError: "INTERNAL_ERROR",
} as const;

export type ErrorCode = (typeof ErrorCode)[keyof typeof ErrorCode];

export class BaseflareError<TData = undefined> extends Error {
  readonly data: TData;

  constructor(data: TData, message?: string) {
    super(message ?? "BaseflareError");
    this.name = "BaseflareError";
    this.data = data;
  }
}
