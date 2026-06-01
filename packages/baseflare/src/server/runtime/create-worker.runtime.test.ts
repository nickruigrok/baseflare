import {
  createExecutionContext,
  env,
  waitOnExecutionContext,
} from "cloudflare:test";
import {
  BaseflareError,
  ErrorCode,
  generateId,
  type Id,
  v,
} from "baseflare/values";
import {
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { action } from "../functions/action";
import { internalMutation } from "../functions/internal-mutation";
import { internalQuery } from "../functions/internal-query";
import { mutation } from "../functions/mutation";
import { query } from "../functions/query";
import { httpAction } from "../http/http-action";
import { httpRouter } from "../http/http-router";
import { defineRules } from "../permissions/define-rules";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { createWorker } from "./create-worker";
import { buildBaseflareManifest } from "./manifest";
import { applyRuntimeSchema } from "./schema-apply";
import type { BaseflareManifest, D1Database } from "./types";

declare module "cloudflare:test" {
  interface ProvidedEnv {
    APP_DB: D1Database;
  }
}

const schema = defineSchema({
  todos: defineTable({
    completed: v.boolean().default(false),
    deletedAt: v.number().optional(),
    note: v.optional(v.union(v.string(), v.null())),
    ownerToken: v.string(),
    rank: v.number().optional(),
    sortValue: v.any().optional(),
    text: v.string(),
  }).index("by_owner", ["ownerToken"]),
});

const missingTableSchema = defineSchema({
  notApplied: defineTable({
    ownerToken: v.string(),
    text: v.string(),
  }),
});

const missingTableRules = defineRules({
  notApplied: {
    read: () => true,
  },
});

let rowConflictAttempts = 0;
let tableConflictAttempts = 0;
let exhaustedConflictAttempts = 0;

const rules = defineRules({
  todos: {
    delete: async ({ ctx, existingDoc }) =>
      (await getToken(ctx)) === existingDoc.ownerToken,
    insert: async ({ ctx, value }) =>
      (await getToken(ctx)) === value.ownerToken,
    read: async ({ ctx, doc }) => (await getToken(ctx)) === doc.ownerToken,
    update: async ({ ctx, existingDoc }) =>
      (await getToken(ctx)) === existingDoc.ownerToken,
  },
});

async function getToken(ctx: unknown): Promise<string | null> {
  const identity = await (
    ctx as {
      auth: {
        getUserIdentity(): Promise<{ token: string } | null>;
      };
    }
  ).auth.getUserIdentity();

  return identity?.token ?? null;
}

const countTodos = internalQuery({
  args: { ownerToken: v.string() },
  returns: v.number(),
  handler(ctx, args) {
    return ctx.db
      .query("todos")
      .filter({ ownerToken: args.ownerToken })
      .count();
  },
});

const createTodoInternal = internalMutation({
  args: { ownerToken: v.string(), text: v.string() },
  returns: v.id("todos"),
  async handler(ctx, args) {
    const id = await ctx.db.insert("todos", {
      ownerToken: args.ownerToken,
      text: args.text,
    });
    return id as Id<"todos">;
  },
});

const listTodos = query({
  args: { ownerToken: v.string() },
  handler(ctx, args) {
    return ctx.db
      .query("todos")
      .filter({ ownerToken: args.ownerToken })
      .order("text", "asc")
      .collect();
  },
});

const permissionShapeProbe = query({
  args: { ownerToken: v.string() },
  async handler(ctx, args) {
    const createQuery = () =>
      ctx.db
        .query("todos")
        .filter({ ownerToken: args.ownerToken })
        .order("text", "asc");

    return {
      count: await createQuery().count(),
      first: await createQuery().first(),
      page: await createQuery().paginate({ cursor: null, numItems: 2 }),
      take: await createQuery().take(2),
    };
  },
});

const filterProbe = query({
  args: { id: v.id("todos"), ownerToken: v.string(), since: v.number() },
  async handler(ctx, args) {
    const createQuery = () =>
      ctx.db.query("todos").filter({ ownerToken: args.ownerToken });

    return {
      byCreatedAt: await createQuery()
        .filter({ _createdAt: { gte: args.since } })
        .order("text", "asc")
        .collect(),
      byId: await createQuery().filter({ _id: args.id }).first(),
      comparison: await createQuery()
        .filter({ rank: { gte: 2, lt: 4 } })
        .order("rank", "asc")
        .collect(),
      inWithNullish: await createQuery()
        .filter({ note: { in: [null, "keep"] } })
        .order("text", "asc")
        .collect(),
      logical: await createQuery()
        .filter({
          AND: [
            { completed: false },
            { OR: [{ rank: { gt: 1 } }, { note: "keep" }] },
            { NOT: { deletedAt: { neq: null } } },
          ],
        })
        .order("text", "asc")
        .collect(),
      neq: await createQuery()
        .filter({ note: { neq: null } })
        .order("text", "asc")
        .collect(),
    };
  },
});

const queryMissingRuntimeTable = query({
  args: {},
  handler(ctx) {
    return ctx.db.query("notApplied").collect();
  },
});

const getTodo = query({
  args: { id: v.id("todos") },
  handler(ctx, args) {
    return ctx.db.get("todos", args.id);
  },
});

const uniqueTodo = query({
  args: { ownerToken: v.string() },
  handler(ctx, args) {
    return ctx.db
      .query("todos")
      .filter({ ownerToken: args.ownerToken })
      .unique();
  },
});

const createTodo = mutation({
  args: { ownerToken: v.string(), text: v.string() },
  returns: v.object({
    count: v.number(),
    id: v.id("todos"),
  }),
  async handler(ctx, args) {
    const id = await ctx.db.insert("todos", {
      ownerToken: args.ownerToken,
      text: args.text,
    });
    const count = await ctx.runQuery(countTodos, {
      ownerToken: args.ownerToken,
    });

    return { count, id: id as Id<"todos"> };
  },
});

const createDetailedTodo = mutation({
  args: {
    completed: v.boolean().default(false),
    deletedAt: v.number().optional(),
    note: v.optional(v.union(v.string(), v.null())),
    ownerToken: v.string(),
    rank: v.number().optional(),
    text: v.string(),
  },
  returns: v.id("todos"),
  async handler(ctx, args) {
    const id = await ctx.db.insert("todos", args);
    return id as Id<"todos">;
  },
});

const patchAndReadTodo = mutation({
  args: { id: v.id("todos"), text: v.string() },
  async handler(ctx, args) {
    await ctx.db.patch("todos", args.id, { text: args.text });
    return ctx.runQuery(getTodo, { id: args.id });
  },
});

const deleteAndVerifyTodo = mutation({
  args: { id: v.id("todos") },
  async handler(ctx, args) {
    await ctx.db.delete("todos", args.id);
    return ctx.runQuery(getTodo, { id: args.id });
  },
});

const createThenFail = mutation({
  args: { ownerToken: v.string(), text: v.string() },
  async handler(ctx, args) {
    await ctx.db.insert("todos", {
      ownerToken: args.ownerToken,
      text: args.text,
    });
    throw new Error("boom");
  },
});

const createThenInvalidReturn = mutation({
  args: { ownerToken: v.string(), text: v.string() },
  returns: v.number(),
  async handler(ctx, args) {
    await ctx.db.insert("todos", {
      ownerToken: args.ownerToken,
      text: args.text,
    });

    return "not-a-number" as unknown as number;
  },
});

const createMixedOrderTodoAndList = mutation({
  args: { ownerToken: v.string() },
  async handler(ctx, args) {
    await ctx.db.insert("todos", {
      ownerToken: args.ownerToken,
      sortValue: "1",
      text: "pending-text",
    });

    const createQuery = () =>
      ctx.db
        .query("todos")
        .filter({ ownerToken: args.ownerToken })
        .order("sortValue", "asc");

    const firstPage = await createQuery().paginate({
      cursor: null,
      numItems: 1,
    });
    const secondPage = await createQuery().paginate({
      cursor: firstPage.continueCursor,
      numItems: 1,
    });

    return {
      ordered: (await createQuery().collect()).map((todo) => todo.text),
      pages: [
        firstPage.page.map((todo) => todo.text),
        secondPage.page.map((todo) => todo.text),
      ],
    };
  },
});

const createObjectOrderTodoAndList = mutation({
  args: { ownerToken: v.string() },
  async handler(ctx, args) {
    await ctx.db.insert("todos", {
      ownerToken: args.ownerToken,
      sortValue: { z: 1, a: 2 },
      text: "pending-object",
    });

    return (
      await ctx.db
        .query("todos")
        .filter({ ownerToken: args.ownerToken })
        .order("sortValue", "asc")
        .collect()
    ).map((todo) => todo.text);
  },
});

const countLimitedTodos = mutation({
  args: { ownerToken: v.string() },
  returns: v.number(),
  handler(ctx, args) {
    return ctx.db
      .query("todos")
      .filter({ ownerToken: args.ownerToken })
      .limit(1)
      .count();
  },
});

const firstTodoText = mutation({
  args: { ownerToken: v.string() },
  returns: v.union(v.string(), v.null()),
  async handler(ctx, args) {
    return (
      await ctx.db
        .query("todos")
        .filter({ ownerToken: args.ownerToken })
        .order("text", "asc")
        .first()
    )?.text as string | null;
  },
});

const uniqueTodoInMutation = mutation({
  args: { ownerToken: v.string() },
  handler(ctx, args) {
    return ctx.db
      .query("todos")
      .filter({ ownerToken: args.ownerToken })
      .unique();
  },
});

const patchWithOneRowConflict = mutation({
  args: { id: v.id("todos"), text: v.string() },
  returns: v.number(),
  async handler(ctx, args) {
    rowConflictAttempts += 1;
    await ctx.db.get("todos", args.id);

    if (rowConflictAttempts === 1) {
      await env.APP_DB.prepare("UPDATE todos SET _rev = _rev + 1 WHERE _id = ?")
        .bind(args.id)
        .run();
    }

    await ctx.db.patch("todos", args.id, { text: args.text });
    return rowConflictAttempts;
  },
});

const patchWithExhaustedRowConflicts = mutation({
  args: { id: v.id("todos"), text: v.string() },
  async handler(ctx, args) {
    exhaustedConflictAttempts += 1;
    await ctx.db.get("todos", args.id);
    await env.APP_DB.prepare("UPDATE todos SET _rev = _rev + 1 WHERE _id = ?")
      .bind(args.id)
      .run();
    await ctx.db.patch("todos", args.id, { text: args.text });
  },
});

const insertWithOneTableConflict = mutation({
  args: { ownerToken: v.string(), text: v.string() },
  returns: v.number(),
  async handler(ctx, args) {
    tableConflictAttempts += 1;
    await ctx.db.query("todos").filter({ ownerToken: args.ownerToken }).count();

    if (tableConflictAttempts === 1) {
      await insertStoredTodo({
        ownerToken: args.ownerToken,
        text: "external-table-conflict",
      });
    }

    await ctx.db.insert("todos", {
      ownerToken: args.ownerToken,
      text: args.text,
    });

    return tableConflictAttempts;
  },
});

const patchAfterMissingTableVersion = mutation({
  args: { id: v.id("todos"), text: v.string() },
  async handler(ctx, args) {
    await ctx.db.get("todos", args.id);
    await env.APP_DB.prepare(
      "DELETE FROM _bf_table_versions WHERE table_name = 'todos'"
    ).run();
    await ctx.db.patch("todos", args.id, { text: args.text });
  },
});

const insertAfterMissingTableVersion = mutation({
  args: { ownerToken: v.string(), text: v.string() },
  async handler(ctx, args) {
    await ctx.db.query("todos").filter({ ownerToken: args.ownerToken }).count();
    await env.APP_DB.prepare(
      "DELETE FROM _bf_table_versions WHERE table_name = 'todos'"
    ).run();
    await ctx.db.insert("todos", args);
  },
});

const relayAction = action({
  args: { ownerToken: v.string(), text: v.string() },
  async handler(ctx, args) {
    const id = await ctx.runMutation(createTodoInternal, args);
    const count = await ctx.runQuery(countTodos, {
      ownerToken: args.ownerToken,
    });
    return { count, id };
  },
});

const directWriteAction = action({
  args: { ownerToken: v.string(), text: v.string() },
  async handler(ctx, args) {
    const id = await ctx.db.insert("todos", args);
    return { id };
  },
});

const structuredErrorAction = action({
  args: {},
  handler() {
    throw new BaseflareError(
      {
        code: ErrorCode.PermissionDenied,
        data: { reason: "blocked" },
      },
      "Blocked"
    );
  },
});

const schedulerProbe = action({
  args: {},
  handler(ctx) {
    return ctx.scheduler.runAfter(1000, structuredErrorAction, {});
  },
});

const router = httpRouter();
router.route({
  handler: httpAction(async () => Response.json({ ok: true })),
  method: "GET",
  path: "/health",
});
router.route({
  handler: httpAction(async () => new Response("custom")),
  method: "POST",
  path: "/api/query/todos:list",
});
router.route({
  handler: httpAction(async () => new Response("custom-get")),
  method: "GET",
  path: "/api/query/todos:list",
});

function createManifest(
  overrides: Partial<Parameters<typeof buildBaseflareManifest>[0]> = {}
): BaseflareManifest {
  return buildBaseflareManifest({
    schema,
    rules,
    http: router,
    queries: [
      { definition: listTodos, exportName: "list", modulePath: "todos" },
      { definition: getTodo, exportName: "get", modulePath: "todos" },
      { definition: uniqueTodo, exportName: "unique", modulePath: "todos" },
      {
        definition: permissionShapeProbe,
        exportName: "permissionShapes",
        modulePath: "todos",
      },
      {
        definition: filterProbe,
        exportName: "filterProbe",
        modulePath: "todos",
      },
    ],
    mutations: [
      { definition: createTodo, exportName: "create", modulePath: "todos" },
      {
        definition: createDetailedTodo,
        exportName: "createDetailed",
        modulePath: "todos",
      },
      {
        definition: patchAndReadTodo,
        exportName: "patchAndRead",
        modulePath: "todos",
      },
      {
        definition: deleteAndVerifyTodo,
        exportName: "deleteAndVerify",
        modulePath: "todos",
      },
      {
        definition: createThenFail,
        exportName: "createThenFail",
        modulePath: "todos",
      },
      {
        definition: createThenInvalidReturn,
        exportName: "createThenInvalidReturn",
        modulePath: "todos",
      },
      {
        definition: createMixedOrderTodoAndList,
        exportName: "createMixedOrderTodoAndList",
        modulePath: "todos",
      },
      {
        definition: createObjectOrderTodoAndList,
        exportName: "createObjectOrderTodoAndList",
        modulePath: "todos",
      },
      {
        definition: countLimitedTodos,
        exportName: "countLimitedTodos",
        modulePath: "todos",
      },
      {
        definition: firstTodoText,
        exportName: "firstTodoText",
        modulePath: "todos",
      },
      {
        definition: uniqueTodoInMutation,
        exportName: "uniqueTodo",
        modulePath: "todos",
      },
      {
        definition: patchWithOneRowConflict,
        exportName: "patchWithOneRowConflict",
        modulePath: "todos",
      },
      {
        definition: patchWithExhaustedRowConflicts,
        exportName: "patchWithExhaustedRowConflicts",
        modulePath: "todos",
      },
      {
        definition: insertWithOneTableConflict,
        exportName: "insertWithOneTableConflict",
        modulePath: "todos",
      },
      {
        definition: patchAfterMissingTableVersion,
        exportName: "patchAfterMissingTableVersion",
        modulePath: "todos",
      },
      {
        definition: insertAfterMissingTableVersion,
        exportName: "insertAfterMissingTableVersion",
        modulePath: "todos",
      },
    ],
    actions: [
      { definition: relayAction, exportName: "relay", modulePath: "todos" },
      {
        definition: directWriteAction,
        exportName: "directWrite",
        modulePath: "todos",
      },
      {
        definition: structuredErrorAction,
        exportName: "structured",
        modulePath: "errors",
      },
      {
        definition: schedulerProbe,
        exportName: "schedulerProbe",
        modulePath: "runtime",
      },
    ],
    internalQueries: [
      { definition: countTodos, exportName: "count", modulePath: "todos" },
    ],
    internalMutations: [
      {
        definition: createTodoInternal,
        exportName: "create",
        modulePath: "todos/internal",
      },
    ],
    ...overrides,
  });
}

const worker = createWorker(createManifest());

async function invoke(
  path: string,
  init: RequestInit = {},
  currentWorker = worker
): Promise<Response> {
  const request = new Request(`http://example.com${path}`, init);
  const ctx = createExecutionContext();
  const response = await currentWorker.fetch(request, env, ctx);
  await waitOnExecutionContext(ctx);
  return response;
}

function rpcBody(args: unknown): string {
  return JSON.stringify({ args });
}

async function createTodoViaRpc(
  ownerToken: string,
  text: string
): Promise<string> {
  const response = await invoke("/api/mutation/todos:create", {
    body: rpcBody({ ownerToken, text }),
    headers: { authorization: `Bearer ${ownerToken}` },
    method: "POST",
  });
  const body = (await response.json()) as { result: { id: string } };
  return body.result.id;
}

async function createDetailedTodoViaRpc(args: {
  completed?: boolean;
  deletedAt?: number;
  note?: null | string;
  ownerToken: string;
  rank?: number;
  text: string;
}): Promise<string> {
  const response = await invoke("/api/mutation/todos:createDetailed", {
    body: rpcBody(args),
    headers: { authorization: `Bearer ${args.ownerToken}` },
    method: "POST",
  });
  const body = (await response.json()) as { result: string };
  return body.result;
}

async function insertStoredTodo(doc: {
  completed?: boolean;
  deletedAt?: number;
  note?: null | string;
  ownerToken: string;
  rank?: number;
  sortValue?: unknown;
  text: string;
}): Promise<string> {
  const id = generateId();
  const data = JSON.stringify({ completed: false, ...doc });

  await env.APP_DB.batch([
    env.APP_DB.prepare(
      "INSERT INTO todos (_id, _data, _rev) VALUES (?, ?, 0)"
    ).bind(id, data),
    env.APP_DB.prepare(
      "UPDATE _bf_table_versions SET version = version + 1 WHERE table_name = ?"
    ).bind("todos"),
  ]);

  return id;
}

async function insertMalformedStoredTodo(text: string): Promise<void> {
  await env.APP_DB.batch([
    env.APP_DB.prepare(
      "INSERT INTO todos (_id, _data, _rev) VALUES (?, ?, 0)"
    ).bind("not-a-uuid-v7", JSON.stringify({ ownerToken: "owner-a", text })),
    env.APP_DB.prepare(
      "UPDATE _bf_table_versions SET version = version + 1 WHERE table_name = ?"
    ).bind("todos"),
  ]);
}

describe("worker runtime", () => {
  beforeAll(async () => {
    await applyRuntimeSchema(env.APP_DB, schema);
  });

  beforeEach(async () => {
    exhaustedConflictAttempts = 0;
    rowConflictAttempts = 0;
    tableConflictAttempts = 0;
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await env.APP_DB.prepare("DELETE FROM todos").run();
    await env.APP_DB.prepare(
      "INSERT OR IGNORE INTO _bf_table_versions (table_name, version) VALUES ('todos', 0)"
    ).run();
    await env.APP_DB.prepare(
      "UPDATE _bf_table_versions SET version = 0 WHERE table_name = 'todos'"
    ).run();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("applies runtime schema with document revision and table versions", async () => {
    const table = await env.APP_DB.prepare(
      "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'todos'"
    ).first<{ sql: string }>();
    const version = await env.APP_DB.prepare(
      "SELECT version FROM _bf_table_versions WHERE table_name = 'todos'"
    ).first<{ version: number }>();
    const index = await env.APP_DB.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'todos_by_owner'"
    ).first<{ name: string }>();

    expect(table?.sql).toContain("_rev INTEGER");
    expect(version?.version).toBe(0);
    expect(index?.name).toBe("todos_by_owner");
  });

  it("keeps schema application deploy-owned", async () => {
    const missingWorker = createWorker(
      buildBaseflareManifest({
        rules: missingTableRules,
        schema: missingTableSchema,
        queries: [
          {
            definition: queryMissingRuntimeTable,
            exportName: "list",
            modulePath: "notApplied",
          },
        ],
      })
    );

    const response = await invoke(
      "/api/query/notApplied:list",
      { body: rpcBody({}), method: "POST" },
      missingWorker
    );
    const body = (await response.json()) as {
      error: { code: string; data?: unknown; message: string };
    };
    const table = await env.APP_DB.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'notApplied'"
    ).first<{ name: string }>();

    expect(response.status).toBe(500);
    expect(body.error).toEqual({
      code: ErrorCode.DatabaseError,
      message: "Database error",
    });
    expect(table).toBeNull();
  });

  it("handles query, mutation, action, and internal function routing", async () => {
    const createResponse = await invoke("/api/mutation/todos:create", {
      body: rpcBody({ ownerToken: "owner-a", text: "alpha" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const createBody = (await createResponse.json()) as {
      result: { count: number; id: string };
    };

    expect(createResponse.status).toBe(200);
    expect(createBody.result.count).toBe(1);

    const queryResponse = await invoke("/api/query/todos:list", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const queryBody = (await queryResponse.json()) as {
      result: Array<{ _createdAt: number; _id: string; text: string }>;
    };

    expect(queryBody.result).toHaveLength(1);
    expect(queryBody.result[0]?._createdAt).toBeTypeOf("number");
    expect(queryBody.result[0]?.text).toBe("alpha");

    const actionResponse = await invoke("/api/action/todos:relay", {
      body: rpcBody({ ownerToken: "owner-a", text: "beta" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const actionBody = (await actionResponse.json()) as {
      result: { count: number };
    };

    expect(actionResponse.status).toBe(200);
    expect(actionBody.result.count).toBe(2);

    const internalResponse = await invoke(
      "/api/mutation/todos/internal:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "hidden" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );

    expect(internalResponse.status).toBe(404);
  });

  it("enforces fail-closed permissions", async () => {
    const deniedWrite = await invoke("/api/mutation/todos:create", {
      body: rpcBody({ ownerToken: "owner-a", text: "secret" }),
      headers: { authorization: "Bearer owner-b" },
      method: "POST",
    });
    const deniedBody = (await deniedWrite.json()) as {
      error: { code: string };
    };

    expect(deniedWrite.status).toBe(403);
    expect(deniedBody.error.code).toBe(ErrorCode.PermissionDenied);

    const id = await createTodoViaRpc("owner-a", "secret");
    const deniedRead = await invoke("/api/query/todos:get", {
      body: rpcBody({ id }),
      headers: { authorization: "Bearer owner-b" },
      method: "POST",
    });
    const deniedReadBody = (await deniedRead.json()) as { result: unknown };

    expect(deniedReadBody.result).toBeNull();

    const workerWithoutRules = createWorker(
      createManifest({ rules: undefined })
    );
    const listResponse = await invoke(
      "/api/query/todos:list",
      {
        body: rpcBody({ ownerToken: "owner-a" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      workerWithoutRules
    );
    const listBody = (await listResponse.json()) as {
      error: { code: string; message: string };
    };

    expect(listResponse.status).toBe(403);
    expect(listBody.error).toEqual({
      code: ErrorCode.PermissionDenied,
      message: "Read rules are not configured",
    });
  });

  it("supports mutation read-your-writes and rollback", async () => {
    const id = await createTodoViaRpc("owner-a", "before");
    const patchResponse = await invoke("/api/mutation/todos:patchAndRead", {
      body: rpcBody({ id, text: "after" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const patchBody = (await patchResponse.json()) as {
      result: { text: string };
    };

    expect(patchBody.result.text).toBe("after");

    const deleteResponse = await invoke("/api/mutation/todos:deleteAndVerify", {
      body: rpcBody({ id }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const deleteBody = (await deleteResponse.json()) as { result: null };

    expect(deleteBody.result).toBeNull();

    const failedResponse = await invoke("/api/mutation/todos:createThenFail", {
      body: rpcBody({ ownerToken: "owner-a", text: "rollback" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });

    expect(failedResponse.status).toBe(500);

    const listResponse = await invoke("/api/query/todos:list", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const listBody = (await listResponse.json()) as {
      result: Array<{ text: string }>;
    };

    expect(listBody.result.some((todo) => todo.text === "rollback")).toBe(
      false
    );
  });

  it("orders mutation read-your-writes with SQLite mixed scalar semantics", async () => {
    await insertStoredTodo({
      ownerToken: "owner-a",
      sortValue: 2,
      text: "base-number",
    });

    const response = await invoke(
      "/api/mutation/todos:createMixedOrderTodoAndList",
      {
        body: rpcBody({ ownerToken: "owner-a" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );
    const body = (await response.json()) as {
      result: {
        ordered: string[];
        pages: string[][];
      };
    };

    expect(body.result.ordered).toEqual(["base-number", "pending-text"]);
    expect(body.result.pages).toEqual([["base-number"], ["pending-text"]]);
  });

  it("orders non-scalar mutation read-your-writes by stored JSON text", async () => {
    await insertStoredTodo({
      ownerToken: "owner-a",
      sortValue: { a: 2, z: 1 },
      text: "base-object",
    });

    const response = await invoke(
      "/api/mutation/todos:createObjectOrderTodoAndList",
      {
        body: rpcBody({ ownerToken: "owner-a" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );
    const body = (await response.json()) as { result: string[] };

    expect(body.result).toEqual(["base-object", "pending-object"]);
  });

  it("does not cap mutation count by query limit", async () => {
    await createTodoViaRpc("owner-a", "first");
    await createTodoViaRpc("owner-a", "second");

    const response = await invoke("/api/mutation/todos:countLimitedTodos", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const body = (await response.json()) as { result: number };

    expect(response.status).toBe(200);
    expect(body.result).toBe(2);
  });

  it("stops limited mutation reads before later malformed rows", async () => {
    await createTodoViaRpc("owner-a", "alpha");
    await insertMalformedStoredTodo("z-malformed");

    const response = await invoke("/api/mutation/todos:firstTodoText", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const body = (await response.json()) as { result: string | null };

    expect(response.status).toBe(200);
    expect(body.result).toBe("alpha");
  });

  it("rolls back writes when return validation fails", async () => {
    const response = await invoke(
      "/api/mutation/todos:createThenInvalidReturn",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "invalid-return" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );
    const listResponse = await invoke("/api/query/todos:list", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const listBody = (await listResponse.json()) as {
      result: Array<{ text: string }>;
    };

    expect(response.status).toBe(400);
    expect(listBody.result.some((todo) => todo.text === "invalid-return")).toBe(
      false
    );
  });

  it("retries row and table-version mutation conflicts", async () => {
    const id = await createTodoViaRpc("owner-a", "before-conflict");

    const rowResponse = await invoke(
      "/api/mutation/todos:patchWithOneRowConflict",
      {
        body: rpcBody({ id, text: "after-row-conflict" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );
    const tableResponse = await invoke(
      "/api/mutation/todos:insertWithOneTableConflict",
      {
        body: rpcBody({
          ownerToken: "owner-a",
          text: "after-table-conflict",
        }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );
    const rowBody = (await rowResponse.json()) as { result: number };
    const tableBody = (await tableResponse.json()) as { result: number };
    const listResponse = await invoke("/api/query/todos:list", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const listBody = (await listResponse.json()) as {
      result: Array<{ text: string }>;
    };

    expect(rowResponse.status).toBe(200);
    expect(tableResponse.status).toBe(200);
    expect(rowBody.result).toBe(2);
    expect(tableBody.result).toBe(2);
    expect(tableConflictAttempts).toBe(2);
    expect(listBody.result.map((todo) => todo.text)).toContain(
      "after-row-conflict"
    );
    expect(listBody.result.map((todo) => todo.text)).toContain(
      "after-table-conflict"
    );
    expect(
      listBody.result.filter((todo) => todo.text === "after-table-conflict")
    ).toHaveLength(1);
  });

  it("returns conflict when mutation retries are exhausted", async () => {
    const id = await createTodoViaRpc("owner-a", "before-exhaustion");
    const before = await env.APP_DB.prepare(
      "SELECT version FROM _bf_table_versions WHERE table_name = 'todos'"
    ).first<{ version: number }>();

    const response = await invoke(
      "/api/mutation/todos:patchWithExhaustedRowConflicts",
      {
        body: rpcBody({ id, text: "after-exhaustion" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );
    const body = (await response.json()) as {
      error: { code: string; message: string };
    };
    const after = await env.APP_DB.prepare(
      "SELECT version FROM _bf_table_versions WHERE table_name = 'todos'"
    ).first<{ version: number }>();

    expect(response.status).toBe(409);
    expect(body.error.code).toBe(ErrorCode.Conflict);
    expect(body.error.message).toBe("Mutation conflict retry limit exceeded");
    expect(exhaustedConflictAttempts).toBe(3);
    expect(after?.version).toBe(before?.version);
  });

  it("keeps successful point-read OCC row-scoped", async () => {
    const id = await createTodoViaRpc("owner-a", "before-missing-row-version");

    const response = await invoke(
      "/api/mutation/todos:patchAfterMissingTableVersion",
      {
        body: rpcBody({ id, text: "after-missing-row-version" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );
    const body = (await response.json()) as {
      error: { code: string; message: string };
    };

    expect(response.status).toBe(500);
    expect(body.error.code).toBe(ErrorCode.InternalError);
    expect(body.error.message).toBe("Internal error");
  });

  it("fails clearly when query-read table-version metadata is missing", async () => {
    const response = await invoke(
      "/api/mutation/todos:insertAfterMissingTableVersion",
      {
        body: rpcBody({
          ownerToken: "owner-a",
          text: "after-missing-table-version",
        }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );
    const body = (await response.json()) as {
      error: { code: string; message: string };
    };

    expect(response.status).toBe(500);
    expect(body.error.code).toBe(ErrorCode.InternalError);
    expect(body.error.message).toBe("Internal error");
  });

  it("keeps permission-filtered query terminals complete", async () => {
    await createTodoViaRpc("owner-b", "a-unreadable");
    await createTodoViaRpc("owner-a", "b-readable");
    await createTodoViaRpc("owner-b", "c-unreadable");
    await createTodoViaRpc("owner-b", "d-unreadable");
    await createTodoViaRpc("owner-a", "e-readable");
    await createTodoViaRpc("owner-b", "f-unreadable");

    const response = await invoke("/api/query/todos:permissionShapes", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const body = (await response.json()) as {
      result: {
        count: number;
        first: { text: string } | null;
        page: { isDone: boolean; page: Array<{ text: string }> };
        take: Array<{ text: string }>;
      };
    };

    expect(body.result.count).toBe(2);
    expect(body.result.first?.text).toBe("b-readable");
    expect(body.result.take.map((todo) => todo.text)).toEqual([
      "b-readable",
      "e-readable",
    ]);
    expect(body.result.page.page.map((todo) => todo.text)).toEqual([
      "b-readable",
      "e-readable",
    ]);
    expect(body.result.page.isDone).toBe(true);
  });

  it("runs object filters, ordering, and pagination through D1", async () => {
    const firstId = await createDetailedTodoViaRpc({
      completed: false,
      note: "keep",
      ownerToken: "owner-a",
      rank: 1,
      text: "alpha",
    });
    await createDetailedTodoViaRpc({
      completed: false,
      note: null,
      ownerToken: "owner-a",
      rank: 2,
      text: "beta",
    });
    await createDetailedTodoViaRpc({
      completed: false,
      ownerToken: "owner-a",
      rank: 3,
      text: "gamma",
    });
    await createDetailedTodoViaRpc({
      completed: true,
      deletedAt: Date.now(),
      note: "archived",
      ownerToken: "owner-a",
      rank: 4,
      text: "omega",
    });
    await createDetailedTodoViaRpc({
      completed: false,
      note: "other",
      ownerToken: "owner-b",
      rank: 2,
      text: "zeta",
    });

    const filterResponse = await invoke("/api/query/todos:filterProbe", {
      body: rpcBody({
        id: firstId,
        ownerToken: "owner-a",
        since: 0,
      }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const pageResponse = await invoke("/api/query/todos:permissionShapes", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const filterBody = (await filterResponse.json()) as {
      result: {
        byCreatedAt: Array<{ text: string }>;
        byId: { text: string } | null;
        comparison: Array<{ text: string }>;
        inWithNullish: Array<{ text: string }>;
        logical: Array<{ text: string }>;
        neq: Array<{ text: string }>;
      };
    };
    const pageBody = (await pageResponse.json()) as {
      result: {
        page: {
          continueCursor: string;
          isDone: boolean;
          page: Array<{ text: string }>;
        };
      };
    };

    expect(filterBody.result.byId?.text).toBe("alpha");
    expect(filterBody.result.byCreatedAt.map((todo) => todo.text)).toEqual([
      "alpha",
      "beta",
      "gamma",
      "omega",
    ]);
    expect(filterBody.result.comparison.map((todo) => todo.text)).toEqual([
      "beta",
      "gamma",
    ]);
    expect(filterBody.result.inWithNullish.map((todo) => todo.text)).toEqual([
      "alpha",
      "beta",
      "gamma",
    ]);
    expect(filterBody.result.logical.map((todo) => todo.text)).toEqual([
      "alpha",
      "beta",
      "gamma",
    ]);
    expect(filterBody.result.neq.map((todo) => todo.text)).toEqual([
      "alpha",
      "omega",
    ]);
    expect(pageBody.result.page.page.map((todo) => todo.text)).toEqual([
      "alpha",
      "beta",
    ]);
    expect(pageBody.result.page.continueCursor).not.toBe("");
  });

  it("returns not found for query unique zero-result lookups", async () => {
    const response = await invoke("/api/query/todos:unique", {
      body: rpcBody({ ownerToken: "missing-owner" }),
      headers: { authorization: "Bearer missing-owner" },
      method: "POST",
    });
    const body = (await response.json()) as {
      error: { code: string; message: string };
    };

    expect(response.status).toBe(404);
    expect(body.error).toEqual({
      code: ErrorCode.NotFound,
      message: "Document not found",
    });
  });

  it("treats query unique duplicate results as known runtime errors", async () => {
    const errorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);

    await createTodoViaRpc("owner-a", "one");
    await createTodoViaRpc("owner-a", "two");
    errorSpy.mockClear();

    const response = await invoke("/api/query/todos:unique", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const body = (await response.json()) as {
      error: { code: string; message: string };
    };

    expect(response.status).toBe(500);
    expect(body.error).toEqual({
      code: ErrorCode.InternalError,
      message: "Internal error",
    });
    expect(errorSpy).not.toHaveBeenCalled();
  });

  it("returns not found for mutation unique zero-result lookups", async () => {
    const response = await invoke("/api/mutation/todos:uniqueTodo", {
      body: rpcBody({ ownerToken: "missing-owner" }),
      headers: { authorization: "Bearer missing-owner" },
      method: "POST",
    });
    const body = (await response.json()) as {
      error: { code: string; message: string };
    };

    expect(response.status).toBe(404);
    expect(body.error).toEqual({
      code: ErrorCode.NotFound,
      message: "Document not found",
    });
  });

  it("treats mutation unique duplicate results as known runtime errors", async () => {
    const errorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);

    await createTodoViaRpc("owner-a", "one");
    await createTodoViaRpc("owner-a", "two");
    errorSpy.mockClear();

    const response = await invoke("/api/mutation/todos:uniqueTodo", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const body = (await response.json()) as {
      error: { code: string; message: string };
    };

    expect(response.status).toBe(500);
    expect(body.error).toEqual({
      code: ErrorCode.InternalError,
      message: "Internal error",
    });
    expect(errorSpy).not.toHaveBeenCalled();
  });

  it("uses stable runtime error envelopes", async () => {
    const malformedBody = await invoke("/api/mutation/todos:create", {
      body: "{bad",
      method: "POST",
    });
    const malformedJson = (await malformedBody.json()) as {
      error: { code: string; message: string };
    };

    expect(malformedBody.status).toBe(400);
    expect(malformedJson.error.code).toBe(ErrorCode.ValidationError);

    const structured = await invoke("/api/action/errors:structured", {
      body: rpcBody({}),
      method: "POST",
    });
    const structuredBody = (await structured.json()) as {
      error: { code: string; data: { reason: string }; message: string };
    };

    expect(structured.status).toBe(403);
    expect(structuredBody.error.code).toBe(ErrorCode.PermissionDenied);
    expect(structuredBody.error.data.reason).toBe("blocked");

    const scheduler = await invoke("/api/action/runtime:schedulerProbe", {
      body: rpcBody({}),
      method: "POST",
    });
    const schedulerBody = (await scheduler.json()) as {
      error: { code: string };
    };

    expect(scheduler.status).toBe(501);
    expect(schedulerBody.error.code).toBe(ErrorCode.NotImplemented);
  });

  it("rejects malformed RPC requests with validation envelopes", async () => {
    const extraKeyResponse = await invoke("/api/query/todos:list", {
      body: JSON.stringify({ args: { ownerToken: "owner-a" }, extra: true }),
      method: "POST",
    });
    const malformedRouteResponse = await invoke("/api/query/%E0%A4%A", {
      body: rpcBody({}),
      method: "POST",
    });
    const oversizedResponse = await invoke("/api/query/todos:list", {
      body: JSON.stringify({ args: "x".repeat(1024 * 1024) }),
      method: "POST",
    });
    const extraKeyBody = (await extraKeyResponse.json()) as {
      error: { code: string };
    };
    const malformedRouteBody = (await malformedRouteResponse.json()) as {
      error: { code: string };
    };
    const oversizedBody = (await oversizedResponse.json()) as {
      error: { code: string };
    };

    expect(extraKeyResponse.status).toBe(400);
    expect(malformedRouteResponse.status).toBe(400);
    expect(oversizedResponse.status).toBe(400);
    expect(extraKeyBody.error.code).toBe(ErrorCode.ValidationError);
    expect(malformedRouteBody.error.code).toBe(ErrorCode.ValidationError);
    expect(oversizedBody.error.code).toBe(ErrorCode.ValidationError);
  });

  it("reports malformed stored documents", async () => {
    const id = generateId();
    await env.APP_DB.prepare("INSERT INTO todos (_id, _data) VALUES (?, ?)")
      .bind(id, "[]")
      .run();

    const response = await invoke("/api/query/todos:get", {
      body: rpcBody({ id }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const body = (await response.json()) as {
      error: { code: string; data: { id: string; tableName: string } };
    };

    expect(response.status).toBe(500);
    expect(body.error.code).toBe(ErrorCode.MalformedDocument);
    expect(body.error.data).toEqual({ id, tableName: "todos" });
  });

  it("lets reserved RPC routes take precedence over custom HTTP routes", async () => {
    await createTodoViaRpc("owner-a", "reserved");

    const rpcResponse = await invoke("/api/query/todos:list", {
      body: rpcBody({ ownerToken: "owner-a" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const rpcBodyJson = (await rpcResponse.json()) as {
      result: Array<{ text: string }>;
    };

    expect(rpcBodyJson.result[0]?.text).toBe("reserved");

    const healthResponse = await invoke("/health", { method: "GET" });
    const healthBody = (await healthResponse.json()) as { ok: boolean };
    const customRpcPrefixResponse = await invoke("/api/query/todos:list", {
      method: "GET",
    });

    expect(healthBody.ok).toBe(true);
    await expect(customRpcPrefixResponse.text()).resolves.toBe("custom-get");
  });

  it("accepts the Worker execution context without changing action contexts", async () => {
    const ctx = createExecutionContext();
    const response = await worker.fetch(
      new Request("http://example.com/health", { method: "GET" }),
      env,
      ctx
    );

    await waitOnExecutionContext(ctx);

    expect(response.status).toBe(200);
  });

  it("returns validation errors for malformed synthetic request URLs", async () => {
    const ctx = createExecutionContext();
    const response = await worker.fetch(
      {
        headers: new Headers(),
        method: "GET",
        url: "not a valid URL",
      } as Request,
      env,
      ctx
    );
    const body = (await response.json()) as {
      error: { code: string; message: string };
    };

    await waitOnExecutionContext(ctx);

    expect(response.status).toBe(400);
    expect(body.error).toEqual({
      code: ErrorCode.ValidationError,
      message: "Request URL is malformed",
    });
  });

  it("bumps table versions for direct action writes", async () => {
    const before = await env.APP_DB.prepare(
      "SELECT version FROM _bf_table_versions WHERE table_name = 'todos'"
    ).first<{ version: number }>();

    const response = await invoke("/api/action/todos:directWrite", {
      body: rpcBody({ ownerToken: "owner-a", text: "action-write" }),
      headers: { authorization: "Bearer owner-a" },
      method: "POST",
    });
    const after = await env.APP_DB.prepare(
      "SELECT version FROM _bf_table_versions WHERE table_name = 'todos'"
    ).first<{ version: number }>();

    expect(response.status).toBe(200);
    expect(after?.version).toBe((before?.version ?? 0) + 1);
  });

  it("requires POST and explicit args bodies for RPC", async () => {
    const getResponse = await invoke("/api/query/todos:missing", {
      method: "GET",
    });
    const emptyBodyResponse = await invoke("/api/query/todos:list", {
      method: "POST",
    });

    expect(getResponse.status).toBe(400);
    expect(emptyBodyResponse.status).toBe(400);
  });
});
