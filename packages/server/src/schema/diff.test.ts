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
    expect(schemaDiff.orphanedTables).toEqual([]);
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

  it("reports orphaned tables but never emits DROP TABLE", () => {
    const current = defineSchema({
      todos: defineTable({ text: v.string() }).index("by_text", ["text"]),
      legacy: defineTable({ value: v.string() }).index("by_value", ["value"]),
    });

    const target = defineSchema({
      todos: defineTable({ text: v.string() }).index("by_text", ["text"]),
    });

    const schemaDiff = diff(current, target);

    expect(schemaDiff.orphanedTables).toEqual(["legacy"]);
    expect(schemaDiff.removedIndexes).toEqual([]);
    expect(schemaDiff.hasChanges).toBe(true);
    expect(schemaDiff.toStatements()).toEqual([]);
  });

  it("recreates an index when its fields change", () => {
    const current = defineSchema({
      todos: defineTable({
        orgId: v.string(),
        status: v.string(),
      }).index("by_org", ["orgId"]),
    });

    const target = defineSchema({
      todos: defineTable({
        orgId: v.string(),
        status: v.string(),
      }).index("by_org", ["orgId", "status"]),
    });

    const schemaDiff = diff(current, target);

    expect(schemaDiff.removedIndexes).toEqual([
      { tableName: "todos", index: { name: "by_org", fields: ["orgId"] } },
    ]);
    expect(schemaDiff.addedIndexes).toEqual([
      {
        tableName: "todos",
        index: { name: "by_org", fields: ["orgId", "status"] },
      },
    ]);
    expect(schemaDiff.toStatements()).toEqual([
      "DROP INDEX IF EXISTS todos_by_org",
      "CREATE INDEX todos_by_org ON todos (json_extract(_data, '$.orgId'), json_extract(_data, '$.status'))",
    ]);
  });
});
