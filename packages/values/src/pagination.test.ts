import { describe, expect, it } from "vitest";

import { paginationOptsValidator } from "./pagination";

const MIN_ITEMS_ERROR_PATTERN = /at least 1/;
const SAFE_INTEGER_ERROR_PATTERN = /safe integer/;

describe("paginationOptsValidator", () => {
  it("defaults cursor to null and enforces numItems", () => {
    expect(paginationOptsValidator.validate({ numItems: 5 })).toEqual({
      numItems: 5,
      cursor: null,
    });

    expect(() => paginationOptsValidator.validate({ numItems: 0 })).toThrow(
      MIN_ITEMS_ERROR_PATTERN
    );
    expect(() => paginationOptsValidator.validate({ numItems: 1.5 })).toThrow(
      SAFE_INTEGER_ERROR_PATTERN
    );
    expect(() =>
      paginationOptsValidator.validate({
        numItems: Number.MAX_SAFE_INTEGER + 1,
      })
    ).toThrow(SAFE_INTEGER_ERROR_PATTERN);
  });
});
