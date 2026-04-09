import { v } from "@baseflare/values";
import { describe, expect, it } from "vitest";

import { defineSchema } from "./define-schema";
import { defineTable } from "./define-table";

const RESERVED_TABLE_NAME_ERROR_PATTERN = /cannot start with "_"/;

describe("defineSchema", () => {
  it("produces document-table and index SQL statements", () => {
    const schema = defineSchema({
      todos: defineTable({
        text: v.string(),
        completed: v.boolean().default(false),
        priority: v.union(
          v.literal("low"),
          v.literal("medium"),
          v.literal("high")
        ),
        assignee: v.optional(v.id("users")),
      }).index("by_completed", ["completed"]),
    });

    expect(schema.toCreateStatements()).toEqual([
      "CREATE TABLE todos (_id TEXT PRIMARY KEY, _data TEXT NOT NULL)",
      "CREATE INDEX todos_by_completed ON todos (json_extract(_data, '$.completed'))",
    ]);
  });

  it("rejects reserved names", () => {
    expect(() =>
      defineSchema({
        _todos: defineTable({ text: v.string() }),
      })
    ).toThrow(RESERVED_TABLE_NAME_ERROR_PATTERN);
  });
});
