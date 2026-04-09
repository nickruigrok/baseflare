import { v } from "@baseflare/values";
import { describe, expect, it } from "vitest";

import { defineSchema } from "./define-schema";
import { defineTable } from "./define-table";
import { diff } from "./diff";

describe("diff", () => {
  it("tracks table and index additions/removals while ignoring field changes", () => {
    const current = defineSchema({
      todos: defineTable({
        text: v.string(),
      }).index("by_text", ["text"]),
    });

    const target = defineSchema({
      todos: defineTable({
        text: v.string(),
        completed: v.boolean().default(false),
      }).index("by_completed", ["completed"]),
      users: defineTable({
        email: v.string(),
      }),
    });

    const schemaDiff = diff(current, target);

    expect(Object.keys(schemaDiff.addedTables)).toEqual(["users"]);
    expect(schemaDiff.removedTables).toEqual([]);
    expect(schemaDiff.addedIndexes).toEqual([
      {
        tableName: "todos",
        index: { name: "by_completed", fields: ["completed"] },
      },
    ]);
    expect(schemaDiff.removedIndexes).toEqual([
      { tableName: "todos", index: { name: "by_text", fields: ["text"] } },
    ]);
  });
});
