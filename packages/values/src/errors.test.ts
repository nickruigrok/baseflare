import { describe, expect, it } from "vitest";

import { BaseflareError, ErrorCode } from "./errors";

describe("BaseflareError", () => {
  it("preserves structured data payloads", () => {
    const error = new BaseflareError({ code: "OUT_OF_STOCK", remaining: 0 });

    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("BaseflareError");
    expect(error.data).toEqual({ code: "OUT_OF_STOCK", remaining: 0 });
  });

  it("exports system error codes", () => {
    expect(ErrorCode.PermissionDenied).toBe("PERMISSION_DENIED");
  });
});
