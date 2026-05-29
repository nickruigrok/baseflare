import { describe, expect, expectTypeOf, it } from "vitest";

import { ValidationError } from "./errors";
import type { Id } from "./types";
import { generateId } from "./uuid";
import { v } from "./validators";

const INVALID_SCHEMA_FIELD_PATTERN = /not allowed by the schema/;
const UUID_V7_ERROR_PATTERN = /UUIDv7/;

describe("validators", () => {
  it("validates primitive and composite values", () => {
    expect(v.string().min(2).max(5).validate("base")).toBe("base");
    expect(v.number().validate(42)).toBe(42);
    expect(v.number().validate(1.5)).toBe(1.5);
    expect(v.boolean().validate(true)).toBe(true);
    expect(v.bytes().validate(new Uint8Array([1, 2, 3]))).toEqual(
      new Uint8Array([1, 2, 3])
    );
    expect(v.null().validate(null)).toBeNull();
    expect(v.array(v.string()).validate(["a", "b"])).toEqual(["a", "b"]);
    expect(v.record(v.number()).validate({ a: 1, b: 2 })).toEqual({
      a: 1,
      b: 2,
    });
    expect(v.union(v.literal("low"), v.literal("high")).validate("high")).toBe(
      "high"
    );
    expect(v.literal("draft").validate("draft")).toBe("draft");
    expect(v.enum(["low", "medium", "high"]).validate("medium")).toBe("medium");
    expect(v.vector({ dimensions: 3 }).validate([1, 2, 3])).toEqual([1, 2, 3]);
    expect(v.any().validate({ anything: true })).toEqual({ anything: true });
  });

  it("applies optional and default semantics to object shapes", () => {
    const todoValidator = v.object({
      text: v.string(),
      completed: v.boolean().default(false),
      assignee: v.optional(v.id("users")),
    });

    const parsed = todoValidator.validate({ text: "ship it" });
    expect(parsed).toEqual({ text: "ship it", completed: false });
    expect("assignee" in parsed).toBe(false);

    expect(() =>
      todoValidator.validate({ text: "ship it", extra: true })
    ).toThrow(INVALID_SCHEMA_FIELD_PATTERN);
  });

  it("throws typed ValidationError with path and code", () => {
    try {
      v.object({ text: v.string() }).validate({ text: 1 });
      throw new Error("expected validation to throw");
    } catch (error) {
      expect(error).toBeInstanceOf(ValidationError);
      const validationError = error as ValidationError;
      expect(validationError.code).toBe("VALIDATION_ERROR");
      expect(validationError.path).toBe("value.text");
    }
  });

  it("uses length-aware messages for bounded collections", () => {
    expect(() => v.string().min(3).validate("hi")).toThrow(
      /must have length at least 3/
    );
    expect(() => v.number().max(10).validate(42)).toThrow(/must be at most 10/);
  });

  it("brands ids by table and validates UUIDv7 format", () => {
    const id = generateId();
    const userIdValidator = v.id("users");
    const userId = userIdValidator.validate(id);
    expect(userId).toBe(id);

    expect(() => v.id("users").validate("not-a-uuid")).toThrow(
      UUID_V7_ERROR_PATTERN
    );

    type UserId = ReturnType<typeof userIdValidator.validate>;
    expectTypeOf<UserId>().toEqualTypeOf<Id<"users">>();
  });
});
