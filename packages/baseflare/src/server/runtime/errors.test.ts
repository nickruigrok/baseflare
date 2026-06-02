import { BaseflareError, ErrorCode } from "baseflare/values";
import { describe, expect, it } from "vitest";

import {
  DatabaseRuntimeError,
  InternalRuntimeError,
  MalformedDocumentRuntimeError,
  toErrorResponse,
} from "./errors";

describe("runtime errors", () => {
  it("sanitizes unexpected errors", async () => {
    const response = toErrorResponse(new Error("secret detail"));
    const body = (await response.json()) as {
      error: { code: string; message: string };
    };

    expect(response.status).toBe(500);
    expect(body.error).toEqual({
      code: ErrorCode.InternalError,
      message: "Internal error",
    });
  });

  it("propagates structured BaseflareError data", async () => {
    const response = toErrorResponse(
      new BaseflareError(
        {
          code: ErrorCode.PermissionDenied,
          data: { reason: "blocked" },
        },
        "Blocked"
      )
    );
    const body = (await response.json()) as {
      error: { code: string; data: { reason: string }; message: string };
    };

    expect(response.status).toBe(403);
    expect(body.error.code).toBe(ErrorCode.PermissionDenied);
    expect(body.error.data.reason).toBe("blocked");
    expect(body.error.message).toBe("Blocked");
  });

  it("keeps runtime error envelopes stable", async () => {
    const databaseResponse = toErrorResponse(
      new DatabaseRuntimeError("SELECT 1")
    );
    const internalResponse = toErrorResponse(
      new InternalRuntimeError("secret internal detail", {
        tableName: "todos",
      })
    );
    const malformedResponse = toErrorResponse(
      new MalformedDocumentRuntimeError("Bad row", {
        id: "doc-id",
        tableName: "todos",
      })
    );
    const databaseBody = (await databaseResponse.json()) as {
      error: { code: string; data?: unknown; message: string };
    };

    expect(databaseResponse.status).toBe(500);
    expect(databaseBody.error).toEqual({
      code: ErrorCode.DatabaseError,
      message: "Database error",
    });
    await expect(internalResponse.json()).resolves.toEqual({
      error: {
        code: ErrorCode.InternalError,
        message: "Internal error",
      },
    });
    expect(malformedResponse.status).toBe(500);
  });
});
