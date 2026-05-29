import { describe, expect, it } from "vitest";

import {
  generateId,
  getCreatedAtFromId,
  getCreatedMsFromId,
  isUuidV7,
  maxIdForMs,
  minIdForMs,
} from "./uuid";

describe("uuid helpers", () => {
  it("generates monotonically increasing UUIDv7 ids", () => {
    const ids = Array.from({ length: 100 }, () => generateId());

    for (let index = 1; index < ids.length; index += 1) {
      const current = ids[index];
      const previous = ids[index - 1];

      expect(current).toBeDefined();
      expect(previous).toBeDefined();
      expect(current && previous ? current > previous : false).toBe(true);
    }
  });

  it("generates parseable UUIDv7 ids", () => {
    const before = Date.now();
    const id = generateId();
    const after = Date.now();

    expect(isUuidV7(id)).toBe(true);

    const createdAt = getCreatedAtFromId(id).getTime();
    expect(createdAt).toBeGreaterThanOrEqual(before - 1);
    expect(createdAt).toBeLessThanOrEqual(after + 1);
  });

  it("derives the creation timestamp in milliseconds", () => {
    const id = generateId();
    expect(getCreatedMsFromId(id)).toBe(getCreatedAtFromId(id).getTime());
  });

  it("builds valid UUIDv7 range sentinels for a timestamp", () => {
    const ms = 1_716_900_000_000;
    const low = minIdForMs(ms);
    const high = maxIdForMs(ms);

    expect(isUuidV7(low)).toBe(true);
    expect(isUuidV7(high)).toBe(true);
    expect(low <= high).toBe(true);
    expect(getCreatedMsFromId(low)).toBe(ms);
    expect(getCreatedMsFromId(high)).toBe(ms);
  });

  it("rejects invalid sentinel timestamps", () => {
    expect(() => minIdForMs(-1)).toThrow();
    expect(() => maxIdForMs(1.5)).toThrow();
  });

  it("orders sentinels so a real id for the same ms falls within range", () => {
    const id = generateId();
    const idMs = getCreatedMsFromId(id);
    expect(minIdForMs(idMs) <= id).toBe(true);
    expect(id <= maxIdForMs(idMs)).toBe(true);
  });
});
