import { describe, expect, it } from "vitest";

import {
  BaseflareError,
  ErrorCode,
  SchemaError,
  ValidationError,
} from "./errors";

describe("BaseflareError", () => {
  it("preserves structured data payloads", () => {
    const error = new BaseflareError({ code: "OUT_OF_STOCK", remaining: 0 });

    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("BaseflareError");
    expect(error.data).toEqual({ code: "OUT_OF_STOCK", remaining: 0 });
    expect(error.message).toBe("BaseflareError");
  });

  it("derives the message from string data", () => {
    const error = new BaseflareError("something broke");
    expect(error.data).toBe("something broke");
    expect(error.message).toBe("something broke");
  });

  it("exports system error codes", () => {
    expect(ErrorCode.PermissionDenied).toBe("PERMISSION_DENIED");
  });
});

describe("ValidationError", () => {
  it("carries a code and path", () => {
    const error = new ValidationError(
      "document.text",
      "document.text must be a string"
    );
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("ValidationError");
    expect(error.code).toBe(ErrorCode.ValidationError);
    expect(error.path).toBe("document.text");
  });
});

describe("SchemaError", () => {
  it("carries a schema error code", () => {
    const error = new SchemaError("Table name cannot start with _");
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("SchemaError");
    expect(error.code).toBe(ErrorCode.SchemaError);
  });
});
