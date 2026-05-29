import { describe, expect, it } from "vitest";

import {
  BaseflareError,
  ErrorCode,
  generateId,
  getCreatedAtFromId,
  paginationOptsValidator,
  v,
} from "./index";

const UUID_V7_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-7/i;

describe("baseflare/values", () => {
  it("exports the phase 1 public API surface", () => {
    expect(BaseflareError).toBeTypeOf("function");
    expect(ErrorCode.ValidationError).toBe("VALIDATION_ERROR");
    expect(v.string().validate("hello")).toBe("hello");
    expect(paginationOptsValidator.validate({ numItems: 10 })).toEqual({
      numItems: 10,
      cursor: null,
    });
  });

  it("generates UUIDv7 ids and derives createdAt values from them", () => {
    const id = generateId();
    expect(id).toMatch(UUID_V7_PATTERN);
    expect(getCreatedAtFromId(id)).toBeInstanceOf(Date);
  });
});
