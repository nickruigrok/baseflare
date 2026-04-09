import { describe, expect, it } from "vitest";

import { generateId, getCreatedAtFromId, isUuidV7 } from "./uuid";

describe("uuid helpers", () => {
  it("generates parseable UUIDv7 ids", () => {
    const before = Date.now();
    const id = generateId();
    const after = Date.now();

    expect(isUuidV7(id)).toBe(true);

    const createdAt = getCreatedAtFromId(id).getTime();
    expect(createdAt).toBeGreaterThanOrEqual(before - 1);
    expect(createdAt).toBeLessThanOrEqual(after + 1);
  });
});
