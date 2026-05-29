import { type Id, v } from "baseflare/values";
import { describe, expect, expectTypeOf, it } from "vitest";

import { internalMutation } from "./internal-mutation";
import { query } from "./query";

const UUID_V7_ERROR_PATTERN = /UUIDv7/;

describe("function wrappers", () => {
  it("capture metadata and validate args/returns", () => {
    const getUser = query({
      args: { id: v.id("users") },
      returns: v.object({ name: v.string(), email: v.string() }),
      handler(_ctx, args) {
        return { name: String(args.id), email: "ada@example.com" };
      },
    });

    expect(getUser.kind).toBe("query");
    expect(getUser.visibility).toBe("public");
    expect(() => getUser.validateArgs({ id: "not-a-uuid" })).toThrow(
      UUID_V7_ERROR_PATTERN
    );
    expect(
      getUser.validateReturn({ name: "Ada", email: "ada@example.com" })
    ).toEqual({
      name: "Ada",
      email: "ada@example.com",
    });
  });

  it("type-infers handler args and marks internal functions as internal", () => {
    const createUser = internalMutation({
      args: { userId: v.id("users") },
      handler(_ctx, args) {
        return args.userId;
      },
    });

    type Args = Parameters<typeof createUser.handler>[1];
    expectTypeOf<Args>().toEqualTypeOf<{ userId: Id<"users"> }>();
    expect(createUser.visibility).toBe("internal");
  });
});
