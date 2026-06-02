import { v } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { query } from "../functions/query";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { createFunctionIndex } from "./function-index";
import { buildBaseflareManifest } from "./manifest";

const schema = defineSchema({
  todos: defineTable({ text: v.string() }),
});

const listTodos = query({
  args: {},
  handler: () => [],
});

describe("runtime manifest", () => {
  it("builds canonical function ids from module path and export name", () => {
    const manifest = buildBaseflareManifest({
      schema,
      queries: [
        {
          definition: listTodos,
          exportName: "list",
          modulePath: "todos",
        },
      ],
    });

    const index = createFunctionIndex(manifest);
    const entry = index.getByName("query", "todos:list");

    expect(entry?.definition).toBe(listTodos);
  });

  it("rejects duplicate canonical function ids", () => {
    expect(() =>
      buildBaseflareManifest({
        schema,
        queries: [
          {
            definition: listTodos,
            exportName: "list",
            modulePath: "todos",
          },
          {
            definition: listTodos,
            exportName: "list",
            modulePath: "todos",
          },
        ],
      })
    ).toThrow(/Duplicate function id/);
  });
});
