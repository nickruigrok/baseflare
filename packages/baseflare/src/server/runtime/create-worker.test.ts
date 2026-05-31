import { v } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { action } from "../functions/action";
import { mutation } from "../functions/mutation";
import { query } from "../functions/query";
import { defineRules } from "../permissions/define-rules";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { PayloadTooLargeRuntimeError } from "./errors";
import { createActionContext } from "./execution";
import { createFunctionIndex } from "./function-index";
import { buildBaseflareManifest } from "./manifest";
import { readRequestBodyText } from "./request-body";
import type {
  D1Database,
  D1DatabaseSession,
  D1PreparedStatement,
  D1Result,
} from "./types";

const schema = defineSchema({
  todos: defineTable({ text: v.string() }),
});
const rules = defineRules({
  todos: {
    insert: () => true,
    read: () => true,
  },
});

class FakePreparedStatement implements D1PreparedStatement {
  private readonly query: string;

  constructor(query: string) {
    this.query = query;
  }

  all<TRow = Record<string, unknown>>() {
    const result = {
      success: true,
      results: this.query.includes("_bf_table_versions")
        ? [{ version: 0 }]
        : [],
    } as unknown as D1Result<TRow>;
    return Promise.resolve(result);
  }

  bind(): D1PreparedStatement {
    return this;
  }

  first() {
    return Promise.resolve(null);
  }

  run() {
    return Promise.resolve({ success: true });
  }
}

describe("worker request body reader", () => {
  it("cancels oversized request body streams", async () => {
    let cancelled = false;
    const request = new Request("http://example.com/api/query/todos:list", {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("too-large"));
        },
        cancel() {
          cancelled = true;
        },
      }),
      duplex: "half",
      method: "POST",
    } as RequestInit);

    await expect(readRequestBodyText(request, 1)).rejects.toBeInstanceOf(
      PayloadTooLargeRuntimeError
    );
    expect(cancelled).toBe(true);
  });

  it("keeps oversized request errors when stream cancellation fails", async () => {
    const request = new Request("http://example.com/api/query/todos:list", {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("too-large"));
        },
        cancel() {
          return Promise.reject(new Error("cancel failed"));
        },
      }),
      duplex: "half",
      method: "POST",
    } as RequestInit);

    await expect(readRequestBodyText(request, 1)).rejects.toBeInstanceOf(
      PayloadTooLargeRuntimeError
    );
  });

  it("uses a primary D1 session for action database access and nested calls", async () => {
    let rootSessionCalls = 0;
    let nestedSessionCalls = 0;
    let sessionConstraint: string | undefined;
    let sessionBatchCalled = false;
    let sessionPrepareCalls = 0;
    const session: D1DatabaseSession & {
      withSession(constraint?: string): D1DatabaseSession;
    } = {
      batch() {
        sessionBatchCalled = true;
        return Promise.resolve([
          { meta: { changes: 1 }, success: true },
          { meta: { changes: 1 }, success: true },
        ]);
      },
      getBookmark() {
        return null;
      },
      prepare(query) {
        sessionPrepareCalls += 1;
        return new FakePreparedStatement(query);
      },
      withSession() {
        nestedSessionCalls += 1;
        return this;
      },
    };
    const database: D1Database = {
      batch() {
        return Promise.resolve([]);
      },
      prepare() {
        throw new Error("Expected action db to use a session");
      },
      withSession(constraint) {
        rootSessionCalls += 1;
        sessionConstraint = constraint;
        return session;
      },
    };
    const getTodoQuery = query({
      args: {},
      handler: (ctx) =>
        ctx.db.get("todos", "019078e5-d29f-7000-8000-000000000001"),
    });
    const getTodoAction = action({
      args: {},
      handler: (ctx) =>
        ctx.db.get("todos", "019078e5-d29f-7000-8000-000000000001"),
    });
    const insertTodoMutation = mutation({
      args: {},
      handler: (ctx) => ctx.db.insert("todos", { text: "nested" }),
    });
    const getTodo = action({
      args: {},
      async handler(ctx) {
        await ctx.db.get("todos", "019078e5-d29f-7000-8000-000000000001");
        await ctx.runQuery(getTodoQuery, {});
        await ctx.runAction(getTodoAction, {});
        await ctx.runMutation(insertTodoMutation, {});
      },
    });

    const ctx = createActionContext({
      database,
      executionContext: {
        waitUntil() {
          // Test execution context stub.
        },
      },
      functionIndex: createFunctionIndex(
        buildBaseflareManifest({
          actions: [
            {
              definition: getTodoAction,
              exportName: "getTodoAction",
              modulePath: "test",
            },
          ],
          mutations: [
            {
              definition: insertTodoMutation,
              exportName: "insertTodoMutation",
              modulePath: "test",
            },
          ],
          queries: [
            {
              definition: getTodoQuery,
              exportName: "getTodoQuery",
              modulePath: "test",
            },
          ],
          schema,
        })
      ),
      requestHeaders: new Headers(),
      rules,
      schema,
    });

    await getTodo.handler(ctx, {});

    expect(sessionConstraint).toBe("first-primary");
    expect(rootSessionCalls).toBe(1);
    expect(nestedSessionCalls).toBe(0);
    expect(sessionPrepareCalls).toBeGreaterThanOrEqual(3);
    expect(sessionBatchCalled).toBe(true);
  });
});
