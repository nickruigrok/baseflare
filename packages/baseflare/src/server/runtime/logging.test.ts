import { describe, expect, it } from "vitest";

import { getRequestLogFields } from "./logging";

describe("runtime logging", () => {
  it("uses a sanitized pathname fallback for malformed request URLs", () => {
    const request = {
      method: "GET",
      url: "not a valid absolute url",
    } as Request;

    expect(getRequestLogFields(request)).toEqual({
      method: "GET",
      pathname: "<malformed>",
    });
  });
});
