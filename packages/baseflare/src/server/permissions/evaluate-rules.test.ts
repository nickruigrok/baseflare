import { describe, expect, it } from "vitest";

import { defineRules } from "./define-rules";
import { evaluateRules } from "./evaluate-rules";

describe("evaluateRules", () => {
  it("denies by default and respects explicit rules", async () => {
    const rules = defineRules({
      todos: {
        read: ({ doc }) => doc.orgId === "org_1",
        insert: ({ value }) => value.orgId === "org_1",
      },
    });

    await expect(
      evaluateRules(rules, {
        tableName: "todos",
        operation: "read",
        ctx: {},
        doc: { orgId: "org_1" },
      })
    ).resolves.toBe(true);

    await expect(
      evaluateRules(rules, {
        tableName: "todos",
        operation: "insert",
        ctx: {},
        value: { orgId: "org_2" },
      })
    ).resolves.toBe(false);

    await expect(
      evaluateRules(rules, {
        tableName: "users",
        operation: "read",
        ctx: {},
        doc: {},
      })
    ).resolves.toBe(false);
  });
});
