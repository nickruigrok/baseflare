import { v } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { action } from "../functions/action";
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
} from "./types";

const schema = defineSchema({
  todos: defineTable({ text: v.string() }),
});

class FakePreparedStatement implements D1PreparedStatement {
  all() {
    return Promise.resolve({ success: true, results: [] });
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

  it("uses a primary D1 session for action database access", async () => {
    let sessionConstraint: string | undefined;
    let sessionPrepareCalled = false;
    const statement = new FakePreparedStatement();
    const session: D1DatabaseSession = {
      batch() {
        return Promise.resolve([]);
      },
      getBookmark() {
        return null;
      },
      prepare() {
        sessionPrepareCalled = true;
        return statement;
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
        sessionConstraint = constraint;
        return session;
      },
    };
    const getTodo = action({
      args: {},
      handler: (ctx) =>
        ctx.db.get("todos", "019078e5-d29f-7000-8000-000000000001"),
    });

    const ctx = createActionContext({
      database,
      executionContext: {
        waitUntil() {
          // Test execution context stub.
        },
      },
      functionIndex: createFunctionIndex(buildBaseflareManifest({ schema })),
      requestHeaders: new Headers(),
      schema,
    });

    await getTodo.handler(ctx, {});

    expect(sessionConstraint).toBe("first-primary");
    expect(sessionPrepareCalled).toBe(true);
  });
});
