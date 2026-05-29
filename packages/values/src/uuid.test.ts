import { describe, expect, it } from "vitest";

import { ValidationError } from "./errors";
import {
  generateId,
  getCreatedAtFromId,
  getCreatedMsFromId,
  isUuidV7,
  maxIdForMs,
  minIdForMs,
} from "./uuid";

const MAX_UUID_V7_TIMESTAMP_MS = 0xff_ff_ff_ff_ff_ff;

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

  it("builds valid UUIDv7 sentinels for boundary timestamps", () => {
    const low = minIdForMs(0);
    const high = maxIdForMs(MAX_UUID_V7_TIMESTAMP_MS);

    expect(isUuidV7(low)).toBe(true);
    expect(isUuidV7(high)).toBe(true);
    expect(getCreatedMsFromId(low)).toBe(0);
    expect(getCreatedMsFromId(high)).toBe(MAX_UUID_V7_TIMESTAMP_MS);
  });

  it("rejects invalid sentinel timestamps", () => {
    for (const value of [
      -1,
      1.5,
      Number.POSITIVE_INFINITY,
      Number.NaN,
      Number.MAX_SAFE_INTEGER,
    ]) {
      expect(() => minIdForMs(value)).toThrow(ValidationError);
      expect(() => maxIdForMs(value)).toThrow(ValidationError);
    }
  });

  it("orders sentinels so a real id for the same ms falls within range", () => {
    const id = generateId();
    const idMs = getCreatedMsFromId(id);
    expect(minIdForMs(idMs) <= id).toBe(true);
    expect(id <= maxIdForMs(idMs)).toBe(true);
  });
});
