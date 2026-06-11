import {
  BaseflareError,
  ErrorCode,
  type RPCError,
  type RPCResponse,
  SchemaError,
  ValidationError,
} from "baseflare/values";

import { logRuntimeEvent } from "./logging";

interface BaseflareErrorData {
  readonly code?: string;
  readonly data?: unknown;
}

interface D1FailureLike {
  readonly success: boolean;
}

export class RuntimeError extends Error {
  readonly data?: unknown;
  readonly runtimeCode: string;
  readonly status: number;

  constructor(code: string, message: string, status: number, data?: unknown) {
    super(message);
    this.name = "RuntimeError";
    this.data = data;
    this.runtimeCode = code;
    this.status = status;
  }
}

export class ValidationRuntimeError extends RuntimeError {
  constructor(message: string, data?: unknown) {
    super(ErrorCode.ValidationError, message, 400, data);
    this.name = "ValidationRuntimeError";
  }
}

export class PayloadTooLargeRuntimeError extends RuntimeError {
  constructor(message = "RPC request body exceeds the internal size limit") {
    super(ErrorCode.ValidationError, message, 400);
    this.name = "PayloadTooLargeRuntimeError";
  }
}

export class UnauthorizedRuntimeError extends RuntimeError {
  constructor(message = "Unauthorized", data?: unknown) {
    super(ErrorCode.Unauthorized, message, 401, data);
    this.name = "UnauthorizedRuntimeError";
  }
}

export class PermissionDeniedRuntimeError extends RuntimeError {
  constructor(message = "Permission denied", data?: unknown) {
    super(ErrorCode.PermissionDenied, message, 403, data);
    this.name = "PermissionDeniedRuntimeError";
  }
}

export class NotFoundRuntimeError extends RuntimeError {
  constructor(message = "Not found", data?: unknown) {
    super(ErrorCode.NotFound, message, 404, data);
    this.name = "NotFoundRuntimeError";
  }
}

export class ConflictRuntimeError extends RuntimeError {
  constructor(message = "Conflict", data?: unknown) {
    super(ErrorCode.Conflict, message, 409, data);
    this.name = "ConflictRuntimeError";
  }
}

export class MalformedDocumentRuntimeError extends RuntimeError {
  constructor(message: string, data: { id: string; tableName: string }) {
    super(ErrorCode.MalformedDocument, message, 500, data);
    this.name = "MalformedDocumentRuntimeError";
  }
}

export class DatabaseRuntimeError extends RuntimeError {
  readonly operation: string;

  constructor(operation: string) {
    super(ErrorCode.DatabaseError, "Database error", 500);
    this.name = "DatabaseRuntimeError";
    this.operation = operation;
  }
}

export class InternalRuntimeError extends RuntimeError {
  constructor(message = "Internal error", data?: unknown) {
    super(ErrorCode.InternalError, message, 500, data);
    this.name = "InternalRuntimeError";
  }
}

export class NotImplementedRuntimeError extends RuntimeError {
  constructor(message: string) {
    super(ErrorCode.NotImplemented, message, 501);
    this.name = "NotImplementedRuntimeError";
  }
}

export function coerceValidationError(error: unknown, label: string): never {
  if (error instanceof RuntimeError) {
    throw error;
  }

  if (error instanceof Error) {
    throw new ValidationRuntimeError(`${label}: ${error.message}`);
  }

  throw new ValidationRuntimeError(label);
}

export function coerceDatabaseError(error: unknown, operation: string): never {
  if (error instanceof RuntimeError) {
    throw error;
  }

  const cause = error instanceof Error ? error.message : undefined;
  logRuntimeEvent("error", "runtime.database_error", { cause, operation });
  throw new DatabaseRuntimeError(operation);
}

export function coerceMalformedDocumentError(
  error: unknown,
  tableName: string,
  id: string
): never {
  if (error instanceof RuntimeError) {
    throw error;
  }

  throw new MalformedDocumentRuntimeError(
    `Stored document "${id}" in table "${tableName}" is malformed`,
    { id, tableName }
  );
}

export async function withDatabaseErrorHandling<TResult>(
  operation: string,
  execute: () => Promise<TResult>
): Promise<TResult> {
  try {
    return await execute();
  } catch (error) {
    coerceDatabaseError(error, operation);
  }
}

export function ensureSuccessfulD1Result<TResult extends D1FailureLike>(
  result: TResult,
  operation: string
): TResult {
  if (!result.success) {
    logRuntimeEvent("error", "runtime.database_result_error", { operation });
    throw new DatabaseRuntimeError(operation);
  }

  return result;
}

export function jsonResult<TResult>(
  result: TResult,
  init?: ResponseInit
): Response {
  return Response.json({ result } satisfies RPCResponse<TResult>, init);
}

function statusForCode(code: string): number {
  if (code === ErrorCode.ValidationError) {
    return 400;
  }

  if (code === ErrorCode.Unauthorized) {
    return 401;
  }

  if (code === ErrorCode.PermissionDenied) {
    return 403;
  }

  if (code === ErrorCode.NotFound) {
    return 404;
  }

  if (code === ErrorCode.Conflict) {
    return 409;
  }

  if (code === ErrorCode.NotImplemented) {
    return 501;
  }

  return 500;
}

function normalizeBaseflareError(error: BaseflareError): {
  code: string;
  data?: unknown;
  message: string;
  status: number;
} {
  const payload =
    typeof error.data === "object" && error.data !== null
      ? (error.data as BaseflareErrorData)
      : undefined;
  const code = payload?.code ?? ErrorCode.InternalError;
  const data = payload && "data" in payload ? payload.data : error.data;

  return {
    code,
    data,
    message: error.message || "BaseflareError",
    status: statusForCode(code),
  };
}

/**
 * Maps any thrown value to the client-safe error contract shared by HTTP
 * responses and realtime socket frames: internal/unknown failures are
 * redacted to "Internal error", while validation-class errors keep their
 * developer-actionable messages.
 */
export function toErrorPayload(error: unknown): {
  readonly payload: RPCError;
  readonly status: number;
} {
  if (error instanceof InternalRuntimeError) {
    return {
      payload: {
        code: ErrorCode.InternalError,
        message: "Internal error",
      },
      status: error.status,
    };
  }

  if (error instanceof RuntimeError) {
    return {
      payload: {
        code: error.runtimeCode,
        data: error.data,
        message: error.message,
      },
      status: error.status,
    };
  }

  if (error instanceof ValidationError || error instanceof SchemaError) {
    return {
      payload: {
        code: error.code,
        message: error.message,
      },
      status: 400,
    };
  }

  if (error instanceof BaseflareError) {
    const normalized = normalizeBaseflareError(error);
    return {
      payload: {
        code: normalized.code,
        data: normalized.data,
        message: normalized.message,
      },
      status: normalized.status,
    };
  }

  return {
    payload: {
      code: ErrorCode.InternalError,
      message: "Internal error",
    },
    status: 500,
  };
}

export function toErrorResponse(error: unknown): Response {
  const { payload, status } = toErrorPayload(error);
  return Response.json({ error: payload }, { status });
}
