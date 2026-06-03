import type { Id } from "baseflare/values";
import { v } from "baseflare/values";
import { describe, expect, expectTypeOf, it } from "vitest";

import { defineSchema } from "./define-schema";
import { defineTable } from "./define-table";
import {
  createIndexStatement,
  type DataModelFromSchema,
  type Doc,
  type TableBuilder,
} from "./types";

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
      "CREATE TABLE todos (_id TEXT PRIMARY KEY, _data TEXT NOT NULL, _rev INTEGER NOT NULL DEFAULT 0 CHECK(_rev >= 0))",
      "CREATE INDEX todos_by_completed ON todos (json_extract(_data, '$.completed'))",
    ]);
  });

  it("rejects reserved names", () => {
    expect(() =>
      defineSchema({
        _todos: defineTable({ text: v.string() }),
      })
    ).toThrow(RESERVED_TABLE_NAME_ERROR_PATTERN);

    for (const name of ["AND", "OR", "NOT"]) {
      expect(() => defineTable({ [name]: v.boolean() })).toThrow(
        /reserved for query filter logic/
      );
    }

    expect(() =>
      defineTable({
        and: v.boolean(),
        or: v.boolean(),
        not: v.boolean(),
      })
    ).not.toThrow();
  });

  it("supports IF NOT EXISTS index statement generation", () => {
    expect(
      createIndexStatement(
        "todos",
        { name: "by_completed", fields: ["completed"] },
        { ifNotExists: true }
      )
    ).toBe(
      "CREATE INDEX IF NOT EXISTS todos_by_completed ON todos (json_extract(_data, '$.completed'))"
    );
  });

  it("records partition index metadata", () => {
    const schema = defineSchema({
      messages: defineTable({
        channelId: v.string(),
        authorId: v.string(),
        text: v.string(),
      })
        .index("by_channel", ["channelId"], { partition: true })
        .index("by_author", ["authorId"], { partition: false }),
    });

    expect(schema.tables.messages.indexes).toEqual([
      { name: "by_channel", fields: ["channelId"], partition: true },
      { name: "by_author", fields: ["authorId"], partition: false },
    ]);
  });

  it("auto-defaults a single index to the partition axis", () => {
    const schema = defineSchema({
      tasks: defineTable({
        projectId: v.string(),
        title: v.string(),
      }).index("by_project", ["projectId"]),
    });

    expect(schema.tables.tasks.indexes).toEqual([
      { name: "by_project", fields: ["projectId"], partition: true },
    ]);
  });

  it("supports opting a single index out of partitioning", () => {
    const schema = defineSchema({
      tasks: defineTable({
        status: v.string(),
        title: v.string(),
      }).index("by_status", ["status"], { partition: false }),
    });

    expect(schema.tables.tasks.indexes).toEqual([
      { name: "by_status", fields: ["status"], partition: false },
    ]);
  });

  it("requires an explicit partition choice for multiple indexes", () => {
    expect(() =>
      defineSchema({
        tasks: defineTable({
          projectId: v.string(),
          status: v.string(),
        })
          .index("by_project", ["projectId"])
          .index("by_status", ["status"]),
      })
    ).toThrow(/mark one index with { partition: true }/);
  });

  it("rejects multiple partition indexes", () => {
    expect(() =>
      defineSchema({
        tasks: defineTable({
          projectId: v.string(),
          ownerId: v.string(),
        })
          .index("by_project", ["projectId"], { partition: true })
          .index("by_owner", ["ownerId"], { partition: true }),
      })
    ).toThrow(/Only one index per table can be partitioned/);
  });

  it("derives document types from the schema without codegen", () => {
    const schema = defineSchema({
      todos: defineTable({
        text: v.string(),
        completed: v.boolean().default(false),
        assignee: v.optional(v.id("users")),
      }).index("by_completed", ["completed"]),
    });

    type Schema = typeof schema;
    type TodoDoc = Doc<Schema, "todos">;
    type TodosBuilder = ReturnType<
      typeof defineTable<typeof schema.tables.todos.fields>
    >;
    type SchemaTableHasTableBuilder =
      "index" extends keyof (typeof schema)["tables"]["todos"] ? true : false;

    expectTypeOf<TodosBuilder>().toExtend<TableBuilder>();
    expectTypeOf<TodoDoc["_id"]>().toEqualTypeOf<Id<"todos">>();
    expectTypeOf<TodoDoc["_createdAt"]>().toEqualTypeOf<number>();
    expectTypeOf<TodoDoc["text"]>().toEqualTypeOf<string>();
    expectTypeOf<TodoDoc["completed"]>().toEqualTypeOf<boolean>();
    expectTypeOf<TodoDoc["assignee"]>().toEqualTypeOf<
      Id<"users"> | undefined
    >();

    type Model = DataModelFromSchema<Schema>;
    expectTypeOf<Model["todos"]>().toEqualTypeOf<TodoDoc>();
    expectTypeOf<SchemaTableHasTableBuilder>().toEqualTypeOf<false>();
  });
});
