import { describe, expect, it } from "vitest";
import {
  assertJsonBounds,
  MAX_JSON_COLLECTION_LENGTH,
  MAX_JSON_DEPTH,
  MAX_JSON_NODES,
  MAX_JSON_STRING_LENGTH,
} from "./json-bounds";

function nestedArray(nestingLevels: number): unknown {
  let value: unknown = 0;
  for (let level = 0; level < nestingLevels; level += 1) {
    value = [value];
  }
  return value;
}

describe("assertJsonBounds", () => {
  // Depth semantics: the root sits at depth 0, so a value with MAX_JSON_DEPTH
  // levels of nesting below the root is "at most MAX_JSON_DEPTH levels deep"
  // and passes; one more level is rejected.
  it("accepts exactly the maximum nesting depth", () => {
    expect(() =>
      assertJsonBounds(nestedArray(MAX_JSON_DEPTH), "Test value")
    ).not.toThrow();
  });

  it("rejects one level beyond the maximum nesting depth", () => {
    expect(() =>
      assertJsonBounds(nestedArray(MAX_JSON_DEPTH + 1), "Test value")
    ).toThrow(`at most ${MAX_JSON_DEPTH} levels deep`);
  });

  it("accepts exactly the maximum node count", () => {
    // Root array + items: 1 + (MAX_JSON_NODES - 1) nodes in total.
    expect(() =>
      assertJsonBounds(new Array(MAX_JSON_NODES - 1).fill(0), "Test value")
    ).not.toThrow();
  });

  it("rejects one node beyond the maximum node count", () => {
    expect(() =>
      assertJsonBounds(new Array(MAX_JSON_NODES).fill(0), "Test value")
    ).toThrow(`at most ${MAX_JSON_NODES} JSON nodes`);
  });

  it("accepts exactly the maximum string length and rejects one more", () => {
    expect(() =>
      assertJsonBounds("x".repeat(MAX_JSON_STRING_LENGTH), "Test value")
    ).not.toThrow();
    expect(() =>
      assertJsonBounds("x".repeat(MAX_JSON_STRING_LENGTH + 1), "Test value")
    ).toThrow(`at most ${MAX_JSON_STRING_LENGTH} characters`);
  });

  it("rejects oversized object key collections", () => {
    const value = Object.fromEntries(
      Array.from({ length: MAX_JSON_COLLECTION_LENGTH + 1 }, (_, index) => [
        `key-${index}`,
        0,
      ])
    );

    expect(() => assertJsonBounds(value, "Test value")).toThrow(
      `at most ${MAX_JSON_COLLECTION_LENGTH} keys`
    );
  });

  it("rejects values with non-plain prototypes", () => {
    expect(() => assertJsonBounds(new Date(), "Test value")).toThrow(
      "JSON-serializable"
    );
  });

  it("accepts null-prototype objects", () => {
    const value = Object.create(null) as Record<string, unknown>;
    value.field = "ok";
    expect(() => assertJsonBounds(value, "Test value")).not.toThrow();
  });
});
