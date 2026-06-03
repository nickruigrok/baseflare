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
import { createFunctionIndex } from "./function-index";
import { buildBaseflareManifest } from "./manifest";
import {
  configureRealtimeRuntime,
  getRealtimeConnectionShardName,
  getRealtimeSubscriptionShardName,
  RealtimeConnectionDO,
  RealtimeSubscriptionDO,
  resetRealtimeRuntimeStateForTest,
} from "./realtime";
import { applyRuntimeSchema } from "./schema-apply";
import type {
  BaseflareManifest,
  BaseflareRuntimeEnv,
  D1Database,
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectStub,
} from "./types";

declare module "cloudflare:test" {
  interface ProvidedEnv {
    APP_DB: D1Database;
  }
}

class FakeDurableObjectNamespace implements DurableObjectNamespace {
  readonly requests: Array<{ name: string; request: Request }> = [];
  private readonly handler: (
    name: string,
    request: Request
  ) => Promise<Response>;

  constructor(handler?: (name: string, request: Request) => Promise<Response>) {
    this.handler =
      handler ?? (() => Promise.resolve(Response.json({ ok: true })));
  }

  get(id: DurableObjectId): DurableObjectStub {
    const name = id.name ?? "unknown";
    return {
      fetch: async (input, init) => {
        const request =
          input instanceof Request ? input : new Request(input, init);
        this.requests.push({ name, request });
        return await this.handler(name, request);
      },
    };
  }

  idFromName(name: string): DurableObjectId {
    return { name };
  }
}

const schema = defineSchema({
  labels: defineTable({
    ownerToken: v.string(),
    text: v.string(),
  }),
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
let multiTableConflictAttempts = 0;

const rules = defineRules({
  labels: {
    delete: async ({ ctx, existingDoc }) =>
      (await getToken(ctx)) === existingDoc.ownerToken,
    insert: async ({ ctx, value }) =>
      (await getToken(ctx)) === value.ownerToken,
    read: async ({ ctx, doc }) => (await getToken(ctx)) === doc.ownerToken,
    update: async ({ ctx, existingDoc }) =>
      (await getToken(ctx)) === existingDoc.ownerToken,
  },
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

const createTodoAndLabelWithOneTableConflict = mutation({
  args: { ownerToken: v.string(), text: v.string() },
  returns: v.number(),
  async handler(ctx, args) {
    multiTableConflictAttempts += 1;
    await ctx.db.query("todos").filter({ ownerToken: args.ownerToken }).count();

    if (multiTableConflictAttempts === 1) {
      await insertStoredTodo({
        ownerToken: args.ownerToken,
        text: "external-multi-table-conflict",
      });
    }

    await ctx.db.insert("todos", {
      ownerToken: args.ownerToken,
      text: args.text,
    });
    await ctx.db.insert("labels", {
      ownerToken: args.ownerToken,
      text: args.text,
    });

    return multiTableConflictAttempts;
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

const mutationWriteAction = action({
  args: { ownerToken: v.string(), text: v.string() },
  async handler(ctx, args) {
    const id = await ctx.runMutation(createTodoInternal, args);
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
        definition: createTodoAndLabelWithOneTableConflict,
        exportName: "createTodoAndLabelWithOneTableConflict",
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
        definition: mutationWriteAction,
        exportName: "writeViaMutation",
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

function createRealtimeRuntimeId(
  manifest: BaseflareManifest = createManifest()
): string {
  return configureRealtimeRuntime({
    functionIndex: createFunctionIndex(manifest),
    rules: manifest.rules,
    schema: manifest.schema,
  });
}

async function invoke(
  path: string,
  init: RequestInit = {},
  currentWorker = worker,
  runtimeEnv: BaseflareRuntimeEnv = env
): Promise<Response> {
  const request = new Request(`http://example.com${path}`, init);
  const ctx = createExecutionContext();
  const response = await currentWorker.fetch(request, runtimeEnv, ctx);
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

async function createRealtimeOutboxEvent(eventId: string): Promise<void> {
  await env.APP_DB.prepare(
    "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
  )
    .bind(eventId, Date.now(), JSON.stringify(["todos"]), JSON.stringify([]))
    .run();
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
    env.APP_DB.prepare(
      "INSERT OR IGNORE INTO _bf_partition_versions (table_name, partition_key, partition_value, version) VALUES (?, ?, ?, 0)"
    ).bind("todos", "by_owner", JSON.stringify([doc.ownerToken])),
    env.APP_DB.prepare(
      "UPDATE _bf_partition_versions SET version = version + 1 WHERE table_name = ? AND partition_key = ? AND partition_value = ?"
    ).bind("todos", "by_owner", JSON.stringify([doc.ownerToken])),
  ]);

  return id;
}

async function countStoredDocuments(
  tableName: "labels" | "todos",
  ownerToken: string,
  text: string
): Promise<number> {
  const row = await env.APP_DB.prepare(
    `SELECT COUNT(*) AS count FROM ${tableName}
     WHERE json_extract(_data, '$.ownerToken') = ?
       AND json_extract(_data, '$.text') = ?`
  )
    .bind(ownerToken, text)
    .first<{ count: number }>();

  return row?.count ?? 0;
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
    resetRealtimeRuntimeStateForTest();
    exhaustedConflictAttempts = 0;
    multiTableConflictAttempts = 0;
    rowConflictAttempts = 0;
    tableConflictAttempts = 0;
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await env.APP_DB.prepare("DELETE FROM labels").run();
    await env.APP_DB.prepare("DELETE FROM todos").run();
    await env.APP_DB.prepare("DELETE FROM _bf_realtime_outbox").run();
    await env.APP_DB.prepare(
      "INSERT OR IGNORE INTO _bf_table_versions (table_name, version) VALUES ('labels', 0)"
    ).run();
    await env.APP_DB.prepare(
      "INSERT OR IGNORE INTO _bf_table_versions (table_name, version) VALUES ('todos', 0)"
    ).run();
    await env.APP_DB.prepare(
      "UPDATE _bf_table_versions SET version = 0 WHERE table_name = 'labels'"
    ).run();
    await env.APP_DB.prepare(
      "UPDATE _bf_table_versions SET version = 0 WHERE table_name = 'todos'"
    ).run();
  });

  afterEach(() => {
    resetRealtimeRuntimeStateForTest();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
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

  it("applies realtime outbox metadata", async () => {
    const outbox = await env.APP_DB.prepare(
      "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '_bf_realtime_outbox'"
    ).first<{ sql: string }>();
    const createdAtIndex = await env.APP_DB.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'index' AND name = '_bf_realtime_outbox_created_at'"
    ).first<{ name: string }>();

    expect(outbox?.sql).toContain("sequence INTEGER PRIMARY KEY AUTOINCREMENT");
    expect(outbox?.sql).toContain("event_id TEXT NOT NULL UNIQUE");
    expect(outbox?.sql).toContain("partitions TEXT NOT NULL");
    expect(createdAtIndex?.name).toBe("_bf_realtime_outbox_created_at");
  });

  it("routes realtime subscribe requests to the connection Durable Object", async () => {
    const connections = new FakeDurableObjectNamespace();
    const response = await invoke(
      "/api/subscribe?clientId=client-a",
      {
        headers: { upgrade: "websocket" },
        method: "GET",
      },
      worker,
      { ...env, REALTIME_CONNECTIONS: connections }
    );

    expect(response.status).toBe(200);
    expect(connections.requests).toHaveLength(1);
    expect(connections.requests[0]?.name).toBe(
      getRealtimeConnectionShardName("client-a")
    );
    expect(new URL(connections.requests[0]?.request.url ?? "").pathname).toBe(
      "/api/subscribe"
    );
  });

  it("keeps future realtime connection shard routing deterministic and bounded", () => {
    const shardCount = 32;
    const first = getRealtimeConnectionShardName("client-a", shardCount);
    const second = getRealtimeConnectionShardName("client-a", shardCount);
    const shardNumber = Number(first.split(":").at(1));

    expect(first).toBe(second);
    expect(first).toMatch(/^connection:\d+$/);
    expect(shardNumber).toBeGreaterThanOrEqual(0);
    expect(shardNumber).toBeLessThan(shardCount);
  });

  it("delivers realtime messages only to sockets for the target connection key", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const clientAMessages: unknown[] = [];
    const clientBMessages: unknown[] = [];
    const responseA = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: { upgrade: "websocket" },
          method: "GET",
        }
      )
    );
    const responseB = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-b",
        {
          headers: { upgrade: "websocket" },
          method: "GET",
        }
      )
    );
    const clientA = (responseA as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const clientB = (responseB as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    clientA.accept?.();
    clientB.accept?.();
    clientA.addEventListener("message", (event) => {
      clientAMessages.push(JSON.parse(String(event.data)) as unknown);
    });
    clientB.addEventListener("message", (event) => {
      clientBMessages.push(JSON.parse(String(event.data)) as unknown);
    });

    const response = await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: "client-a",
          result: [{ text: "only-a" }],
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as { delivered: number };
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(body.delivered).toBe(1);
    expect(clientAMessages).toEqual([
      {
        message: {
          result: [{ text: "only-a" }],
          subscriptionId: "sub-a",
        },
        type: "delivery",
      },
    ]);
    expect(clientBMessages).toEqual([]);

    const missingResponse = await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: "client-missing",
          result: [],
          subscriptionId: "sub-missing",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const missingBody = (await missingResponse.json()) as {
      delivered: number;
    };

    expect(missingBody.delivered).toBe(0);
  });

  it("continues realtime delivery after one socket send fails", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const deliveredPayloads: string[] = [];
    const failedSocket = {
      send() {
        throw new Error("Socket closed");
      },
    } as unknown as WebSocket;
    const activeSocket = {
      send(payload: string) {
        deliveredPayloads.push(payload);
      },
    } as unknown as WebSocket;
    const internals = connectionDo as unknown as {
      socketConnectionKeys: Map<WebSocket, string>;
      socketConnectionNames: Map<WebSocket, string>;
      sockets: Set<WebSocket>;
      socketsByConnectionKey: Map<string, Set<WebSocket>>;
    };
    internals.sockets.add(failedSocket);
    internals.sockets.add(activeSocket);
    internals.socketConnectionKeys.set(failedSocket, "client-a");
    internals.socketConnectionKeys.set(activeSocket, "client-a");
    internals.socketConnectionNames.set(failedSocket, "connection:0");
    internals.socketConnectionNames.set(activeSocket, "connection:0");
    internals.socketsByConnectionKey.set(
      "client-a",
      new Set([failedSocket, activeSocket])
    );

    const response = await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: "client-a",
          result: [{ text: "still-delivered" }],
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as { delivered: number };

    expect(body.delivered).toBe(1);
    expect(deliveredPayloads.map((payload) => JSON.parse(payload))).toEqual([
      {
        message: {
          result: [{ text: "still-delivered" }],
          subscriptionId: "sub-a",
        },
        type: "delivery",
      },
    ]);
    expect(internals.sockets.has(failedSocket)).toBe(false);
    expect(internals.socketsByConnectionKey.get("client-a")).toEqual(
      new Set([activeSocket])
    );
  });

  it("restores realtime subscriptions without serial register round trips", async () => {
    let activeRegistrations = 0;
    let maxActiveRegistrations = 0;
    const registerRequests: unknown[] = [];
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        activeRegistrations += 1;
        maxActiveRegistrations = Math.max(
          maxActiveRegistrations,
          activeRegistrations
        );
        registerRequests.push(await request.json());
        await new Promise((resolve) => setTimeout(resolve, 10));
        activeRegistrations -= 1;
        return Response.json({ ok: true });
      }
    );
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptionDo,
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: { upgrade: "websocket" },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{ failed?: unknown[]; type: string }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as { failed?: unknown[]; type: string }
      );
    });

    client.send(
      JSON.stringify({
        subscriptions: [
          {
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-a",
          },
          {
            args: { ownerToken: "owner-b" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-b",
          },
        ],
        type: "restore",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 3; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(registerRequests).toHaveLength(2);
    expect(maxActiveRegistrations).toBe(2);
    expect(messages.map((message) => message.type).sort()).toEqual([
      "restored",
      "subscribed",
      "subscribed",
    ]);
    expect(messages.at(-1)).toEqual({ failed: [], type: "restored" });
  });

  it("reports partial realtime restore failures after successful registrations", async () => {
    const registerRequests: unknown[] = [];
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        registerRequests.push(await request.json());
        return Response.json({ ok: true });
      }
    );
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptionDo,
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: { upgrade: "websocket" },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{
      failed?: Array<{ error: string; index: number; subscriptionId?: string }>;
      subscriptionId?: string;
      type: string;
    }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as {
          failed?: Array<{
            error: string;
            index: number;
            subscriptionId?: string;
          }>;
          subscriptionId?: string;
          type: string;
        }
      );
    });

    client.send(
      JSON.stringify({
        subscriptions: [
          {
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-good",
          },
          {
            args: { ownerToken: "owner-a" },
            epoch: 1,
            subscriptionId: "sub-bad",
          },
        ],
        type: "restore",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 2; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(registerRequests).toHaveLength(1);
    expect(messages[0]).toEqual({
      subscriptionId: "sub-good",
      type: "subscribed",
    });
    expect(messages[1]).toEqual({
      failed: [
        {
          error: 'Realtime field "queryName" must be a non-empty string',
          index: 1,
          subscriptionId: "sub-bad",
        },
      ],
      type: "restored",
    });
  });

  it("reports realtime restore failures when registration returns an error response", async () => {
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const registration = (await request.json()) as {
          subscriptionId: string;
        };
        return Response.json(
          { ok: false },
          { status: registration.subscriptionId === "sub-bad" ? 500 : 200 }
        );
      }
    );
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptionDo,
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: { upgrade: "websocket" },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{
      failed?: Array<{ error: string; index: number; subscriptionId?: string }>;
      subscriptionId?: string;
      type: string;
    }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as {
          failed?: Array<{
            error: string;
            index: number;
            subscriptionId?: string;
          }>;
          subscriptionId?: string;
          type: string;
        }
      );
    });

    client.send(
      JSON.stringify({
        subscriptions: [
          {
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-good",
          },
          {
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-bad",
          },
        ],
        type: "restore",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 2; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(messages).toEqual([
      { subscriptionId: "sub-good", type: "subscribed" },
      {
        failed: [
          {
            error: "Realtime subscription registration failed with status 500",
            index: 1,
            subscriptionId: "sub-bad",
          },
        ],
        type: "restored",
      },
    ]);
  });

  it("does not report direct realtime subscriptions as live after registration errors", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(() =>
        Promise.resolve(Response.json({ ok: false }, { status: 500 }))
      ),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: { upgrade: "websocket" },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{
      error?: string;
      subscriptionId?: string;
      type: string;
    }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as {
          error?: string;
          subscriptionId?: string;
          type: string;
        }
      );
    });

    client.send(
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 1,
        queryName: "todos:list",
        subscriptionId: "sub-failed",
        type: "subscribe",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(messages).toEqual([
      {
        error: "Realtime subscription registration failed with status 500",
        type: "error",
      },
    ]);
  });

  it("does not confirm realtime unsubscribe after unregister errors", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(() =>
        Promise.resolve(Response.json({ ok: false }, { status: 500 }))
      ),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: { upgrade: "websocket" },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{
      error?: string;
      subscriptionId?: string;
      type: string;
    }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as {
          error?: string;
          subscriptionId?: string;
          type: string;
        }
      );
    });

    client.send(
      JSON.stringify({
        subscriptionId: "sub-failed",
        type: "unsubscribe",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(messages).toEqual([
      {
        error: "Realtime subscription unregister failed with status 500",
        type: "error",
      },
    ]);
  });

  it("keeps malformed realtime restore envelopes on the socket error path", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: { upgrade: "websocket" },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{ error?: string; type: string }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as { error?: string; type: string }
      );
    });

    client.send(JSON.stringify({ subscriptions: {}, type: "restore" }));
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(messages).toEqual([
      {
        error: 'Realtime field "subscriptions" must be an array',
        type: "error",
      },
    ]);
  });

  it("keeps realtime registration failures from blocking other subscribers", async () => {
    const errorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        deliveries.push(await request.json());
        return Response.json({ delivered: 1, ok: true });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (subscriptionId: string, queryName: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/register", {
          body: JSON.stringify({
            args: { ownerToken: "owner-a" },
            authorizationHeader: "Bearer owner-a",
            connectionKey: "client-a",
            connectionName: "connection:0",
            epoch: 1,
            leaseExpiresAt: Date.now() + 60_000,
            queryName,
            runtimeId,
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await register("sub-bad", "todos:renamed");
    await register("sub-good", "todos:list");
    await createTodoViaRpc("owner-a", "still-delivered");
    await createRealtimeOutboxEvent("event-1");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "event-1" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(response.status).toBe(200);
    expect(body).toEqual({ evaluated: 1, failed: 1, ok: true });
    expect(deliveries).toHaveLength(1);
    expect(
      (
        deliveries[0] as {
          result: Array<{ text: string }>;
          subscriptionId: string;
        }
      ).subscriptionId
    ).toBe("sub-good");
    expect(
      (
        deliveries[0] as {
          result: Array<{ text: string }>;
          subscriptionId: string;
        }
      ).result.map((todo) => todo.text)
    ).toEqual(["still-delivered"]);
    expect(errorSpy).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_registration_re_evaluation_failed",
        queryName: "todos:renamed",
        subscriptionId: "sub-bad",
      })
    );
  });

  it("writes realtime outbox events and notifies subscription DOs after mutations", async () => {
    const subscriptions = new FakeDurableObjectNamespace();
    const response = await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "realtime" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, REALTIME_SUBSCRIPTIONS: subscriptions }
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    const row = await env.APP_DB.prepare(
      "SELECT sequence, event_id, tables, partitions FROM _bf_realtime_outbox ORDER BY sequence DESC LIMIT 1"
    ).first<{
      event_id: string;
      partitions: string;
      sequence: number;
      tables: string;
    }>();

    expect(response.status).toBe(200);
    expect(row?.sequence).toBeTypeOf("number");
    expect(row?.event_id).toBeTypeOf("string");
    expect(JSON.parse(row?.tables ?? "[]")).toEqual(["todos"]);
    expect(JSON.parse(row?.partitions ?? "[]")).toEqual([
      {
        partitionKey: "by_owner",
        partitionValue: JSON.stringify(["owner-a"]),
        tableName: "todos",
      },
    ]);
    expect(subscriptions.requests).toHaveLength(1);
    expect(subscriptions.requests[0]?.name).toBe(
      getRealtimeSubscriptionShardName()
    );
    expect(new URL(subscriptions.requests[0]?.request.url ?? "").pathname).toBe(
      "/notify"
    );
  });

  it("uses one realtime subscription shard for mutation notifications", async () => {
    const subscriptions = new FakeDurableObjectNamespace();

    await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "first-realtime-event" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, REALTIME_SUBSCRIPTIONS: subscriptions }
    );
    await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "second-realtime-event" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, REALTIME_SUBSCRIPTIONS: subscriptions }
    );
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(subscriptions.requests).toHaveLength(2);
    expect(subscriptions.requests.map((request) => request.name)).toEqual([
      getRealtimeSubscriptionShardName(),
      getRealtimeSubscriptionShardName(),
    ]);
    expect(
      subscriptions.requests.map(
        (request) => new URL(request.request.url).pathname
      )
    ).toEqual(["/notify", "/notify"]);
  });

  it("lets subscription Durable Objects catch up from the realtime outbox", async () => {
    await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "catch-up" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace() }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: null, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      events: Array<{ sequence: number; tables: string[] }>;
    };

    expect(response.status).toBe(200);
    expect(body.events).toHaveLength(1);
    expect(body.events[0]?.sequence).toBeTypeOf("number");
    expect(body.events[0]?.tables).toEqual(["todos"]);

    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      lastSeenSequence: number | null;
    };

    expect(registrationsBody.lastSeenSequence).toBe(body.events[0]?.sequence);
  });

  it("skips realtime re-evaluation when catch-up has no events", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: null, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      events: unknown[];
      failed: number;
      ok: boolean;
    };

    expect(body).toEqual({ evaluated: 0, events: [], failed: 0, ok: true });
    expect(connections.requests).toHaveLength(0);
  });

  it("re-evaluates active realtime registrations during catch-up", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        deliveries.push(await request.json());
        return Response.json({ delivered: 1, ok: true });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (subscriptionId: string, queryName: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/register", {
          body: JSON.stringify({
            args: { ownerToken: "owner-a" },
            authorizationHeader: "Bearer owner-a",
            connectionKey: "client-a",
            connectionName: "connection:0",
            epoch: 1,
            leaseExpiresAt: Date.now() + 60_000,
            queryName,
            runtimeId,
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await register("sub-bad", "todos:renamed");
    await register("sub-good", "todos:list");
    await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "catch-up-delivered" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace() }
    );

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: null, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      events: Array<{ sequence: number }>;
      failed: number;
      ok: boolean;
    };

    expect(response.status).toBe(200);
    expect(body.events).toHaveLength(1);
    expect(body).toEqual(
      expect.objectContaining({ evaluated: 1, failed: 1, ok: true })
    );
    expect(deliveries).toHaveLength(1);
    expect(
      (
        deliveries[0] as {
          result: Array<{ text: string }>;
          subscriptionId: string;
        }
      ).subscriptionId
    ).toBe("sub-good");
    expect(
      (
        deliveries[0] as {
          result: Array<{ text: string }>;
        }
      ).result.map((todo) => todo.text)
    ).toEqual(["catch-up-delivered"]);
  });

  it("uses realtime outbox sequence instead of event id for catch-up", async () => {
    await env.APP_DB.batch([
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind("z-event", 1, JSON.stringify(["todos"]), JSON.stringify([])),
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind("a-event", 2, JSON.stringify(["labels"]), JSON.stringify([])),
    ]);
    const firstRow = await env.APP_DB.prepare(
      "SELECT sequence FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("z-event")
      .first<{ sequence: number }>();
    const secondRow = await env.APP_DB.prepare(
      "SELECT sequence FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("a-event")
      .first<{ sequence: number }>();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: firstRow?.sequence, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      events: Array<{ eventId: string; sequence: number; tables: string[] }>;
    };

    expect(response.status).toBe(200);
    expect(body.events).toEqual([
      {
        eventId: "a-event",
        partitions: [],
        sequence: secondRow?.sequence,
        tables: ["labels"],
      },
    ]);
  });

  it("advances realtime notify cursors by outbox sequence", async () => {
    await env.APP_DB.prepare(
      "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
    )
      .bind("notify-event", 1, JSON.stringify(["todos"]), JSON.stringify([]))
      .run();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "notify-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const body = (await registrationsResponse.json()) as {
      lastSeenSequence: number | null;
    };
    const row = await env.APP_DB.prepare(
      "SELECT sequence FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("notify-event")
      .first<{ sequence: number }>();

    expect(response.status).toBe(200);
    expect(body.lastSeenSequence).toBe(row?.sequence);
  });

  it("skips realtime re-evaluation when notify references an unknown event", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          deliveries.push(await request.json());
          return Response.json({ ok: true });
        }
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "missing-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      lastSeenSequence: number | null;
    };

    expect(body).toEqual({ evaluated: 0, failed: 0, ok: true });
    expect(deliveries).toHaveLength(0);
    expect(registrationsBody.lastSeenSequence).toBeNull();
  });

  it("keeps realtime notify cursors monotonic for out-of-order events", async () => {
    await env.APP_DB.batch([
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind("older-event", 1, JSON.stringify(["todos"]), JSON.stringify([])),
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind("newer-event", 2, JSON.stringify(["todos"]), JSON.stringify([])),
    ]);
    const newerRow = await env.APP_DB.prepare(
      "SELECT sequence FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("newer-event")
      .first<{ sequence: number }>();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const notify = (eventId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await notify("newer-event");
    await notify("older-event");

    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const body = (await registrationsResponse.json()) as {
      lastSeenSequence: number | null;
    };

    expect(body.lastSeenSequence).toBe(newerRow?.sequence);
  });

  it("does not regress realtime cursors when catch-up returns older events", async () => {
    await env.APP_DB.batch([
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind("catchup-older", 1, JSON.stringify(["todos"]), JSON.stringify([])),
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind("catchup-newer", 2, JSON.stringify(["todos"]), JSON.stringify([])),
    ]);
    const newerRow = await env.APP_DB.prepare(
      "SELECT sequence FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("catchup-newer")
      .first<{ sequence: number }>();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });

    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "catchup-newer" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: null, limit: 1 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const body = (await registrationsResponse.json()) as {
      lastSeenSequence: number | null;
    };

    expect(body.lastSeenSequence).toBe(newerRow?.sequence);
  });

  it("ignores stale realtime registration epochs and keeps leases for delivery cleanup", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (
      epoch: number,
      queryName: string,
      leaseExpiresAt: number
    ) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/register", {
          body: JSON.stringify({
            args: {},
            connectionKey: "client-a",
            connectionName: "connection:0",
            epoch,
            leaseExpiresAt,
            queryName,
            runtimeId,
            subscriptionId: "sub-1",
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await register(2, "todos:new", Date.now() + 60_000);
    await register(1, "todos:stale", Date.now() + 60_000);
    let response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    let body = (await response.json()) as {
      registrations: Array<{ queryName: string }>;
    };

    expect(
      body.registrations.map((registration) => registration.queryName)
    ).toEqual(["todos:new"]);

    await register(3, "todos:expired", Date.now() - 1);
    response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    body = (await response.json()) as {
      registrations: Array<{ queryName: string }>;
    };

    expect(
      body.registrations.map((registration) => registration.queryName)
    ).toEqual(["todos:expired"]);
  });

  it("scopes identical subscription ids by connection key", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (connectionKey: string, queryName: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/register", {
          body: JSON.stringify({
            args: {},
            connectionKey,
            connectionName: "connection:0",
            epoch: 1,
            leaseExpiresAt: Date.now() + 60_000,
            queryName,
            runtimeId,
            subscriptionId: "sub-1",
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await register("client-a", "todos:a");
    await register("client-b", "todos:b");

    let response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    let body = (await response.json()) as {
      registrations: Array<{ connectionKey: string; queryName: string }>;
    };

    expect(
      body.registrations.map((registration) => [
        registration.connectionKey,
        registration.queryName,
      ])
    ).toEqual([
      ["client-a", "todos:a"],
      ["client-b", "todos:b"],
    ]);

    response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/unregister", {
        body: JSON.stringify({
          connectionKey: "client-a",
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    expect(response.status).toBe(200);

    response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    body = (await response.json()) as {
      registrations: Array<{ connectionKey: string; queryName: string }>;
    };

    expect(
      body.registrations.map((registration) => [
        registration.connectionKey,
        registration.queryName,
      ])
    ).toEqual([["client-b", "todos:b"]]);
  });

  it("re-evaluates registered realtime queries and delivers changed results", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        deliveries.push(await request.json());
        return Response.json({ delivered: 1, ok: true });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "delivered");
    await createRealtimeOutboxEvent("event-1");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "event-1" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(response.status).toBe(200);
    expect(connections.requests).toHaveLength(1);
    expect(new URL(connections.requests[0]?.request.url ?? "").pathname).toBe(
      "/deliver"
    );
    expect(deliveries).toHaveLength(1);
    expect(
      (
        deliveries[0] as {
          connectionKey: string;
          result: Array<{ text: string }>;
          subscriptionId: string;
        }
      ).subscriptionId
    ).toBe("sub-1");
    expect(
      (
        deliveries[0] as {
          connectionKey: string;
          result: Array<{ text: string }>;
          subscriptionId: string;
        }
      ).connectionKey
    ).toBe("client-a");
    expect(
      (
        deliveries[0] as {
          result: Array<{ text: string }>;
          subscriptionId: string;
        }
      ).result.map((todo) => todo.text)
    ).toEqual(["delivered"]);
  });

  it("renews expired realtime leases after successful delivery", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace(() =>
      Promise.resolve(Response.json({ delivered: 1, ok: true }))
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() - 1,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "renewed-lease");
    await createRealtimeOutboxEvent("renew-lease-event");
    const renewalStartedAt = Date.now();

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "renew-lease-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(body).toEqual({ evaluated: 1, failed: 0, ok: true });
    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      registrations: Array<{ leaseExpiresAt: number }>;
    };

    expect(registrationsBody.registrations).toHaveLength(1);
    expect(registrationsBody.registrations[0]?.leaseExpiresAt).toBeGreaterThan(
      renewalStartedAt
    );
  });

  it("retries realtime delivery when no socket receives the result", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace(() =>
      Promise.resolve(Response.json({ delivered: 0, ok: true }))
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "retry-without-socket");
    await createRealtimeOutboxEvent("no-socket-retry-event");

    const notify = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId: "no-socket-retry-event" }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    const firstResponse = await notify();
    const firstBody = (await firstResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    const secondResponse = await notify();
    const secondBody = (await secondResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(firstBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(secondBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(connections.requests).toHaveLength(2);
  });

  it("removes expired realtime registrations when no socket receives delivery", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace(() =>
      Promise.resolve(Response.json({ delivered: 0, ok: true }))
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() - 1,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "cleanup-expired-lease");
    await createRealtimeOutboxEvent("cleanup-expired-lease-event");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "cleanup-expired-lease-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(body).toEqual({ evaluated: 1, failed: 0, ok: true });
    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      registrations: unknown[];
    };

    expect(registrationsBody.registrations).toEqual([]);
  });

  it("retries unchanged realtime results after a failed delivery", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    let deliveryAttempts = 0;
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        deliveryAttempts += 1;
        if (deliveryAttempts === 1) {
          throw new Error("Transient delivery failure");
        }

        deliveries.push(await request.json());
        return Response.json({ delivered: 1, ok: true });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "retry-delivery");
    await createRealtimeOutboxEvent("event-1");

    const notify = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId: "event-1" }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    const failedResponse = await notify();
    const failedBody = (await failedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(failedBody).toEqual({ evaluated: 0, failed: 1, ok: true });
    expect(deliveries).toHaveLength(0);

    const retriedResponse = await notify();
    const retriedBody = (await retriedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(retriedBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
    expect(
      (
        deliveries[0] as {
          result: Array<{ text: string }>;
        }
      ).result.map((todo) => todo.text)
    ).toEqual(["retry-delivery"]);

    const duplicateResponse = await notify();
    const duplicateBody = (await duplicateResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(duplicateBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
  });

  it("retries unchanged realtime results after a non-ok delivery response", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    let deliveryAttempts = 0;
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        deliveryAttempts += 1;
        if (deliveryAttempts === 1) {
          return Response.json({ ok: false }, { status: 500 });
        }

        deliveries.push(await request.json());
        return Response.json({ delivered: 1, ok: true });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "retry-non-ok-delivery");
    await createRealtimeOutboxEvent("event-1");

    const notify = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId: "event-1" }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    const failedResponse = await notify();
    const failedBody = (await failedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(failedBody).toEqual({ evaluated: 0, failed: 1, ok: true });
    expect(deliveries).toHaveLength(0);

    const retriedResponse = await notify();
    const retriedBody = (await retriedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(retriedBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
    expect(
      (
        deliveries[0] as {
          result: Array<{ text: string }>;
        }
      ).result.map((todo) => todo.text)
    ).toEqual(["retry-non-ok-delivery"]);

    const duplicateResponse = await notify();
    const duplicateBody = (await duplicateResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(duplicateBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
  });

  it("keeps non-expired realtime registrations after non-ok delivery responses", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace(() =>
      Promise.resolve(Response.json({ ok: false }, { status: 500 }))
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "keep-after-non-ok-delivery");
    await createRealtimeOutboxEvent("keep-after-non-ok-delivery-event");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "keep-after-non-ok-delivery-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      registrations: Array<{ subscriptionId: string }>;
    };

    expect(body).toEqual({ evaluated: 0, failed: 1, ok: true });
    expect(
      registrationsBody.registrations.map(
        (registration) => registration.subscriptionId
      )
    ).toEqual(["sub-1"]);
  });

  it("removes expired realtime registrations after non-ok delivery responses", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace(() =>
      Promise.resolve(Response.json({ ok: false }, { status: 500 }))
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() - 1,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "remove-after-non-ok-delivery");
    await createRealtimeOutboxEvent("remove-after-non-ok-delivery-event");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "remove-after-non-ok-delivery-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      registrations: unknown[];
    };

    expect(body).toEqual({ evaluated: 0, failed: 1, ok: true });
    expect(registrationsBody.registrations).toEqual([]);
  });

  it("removes expired realtime registrations after thrown delivery failures", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace(() => {
      throw new Error("Delivery failed");
    });
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() - 1,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "remove-after-thrown-delivery");
    await createRealtimeOutboxEvent("remove-after-thrown-delivery-event");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "remove-after-thrown-delivery-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      registrations: unknown[];
    };

    expect(body).toEqual({ evaluated: 0, failed: 1, ok: true });
    expect(registrationsBody.registrations).toEqual([]);
  });

  it("keeps realtime registrations bound to their original worker runtime", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        deliveries.push(await request.json());
        return Response.json({ delivered: 1, ok: true });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    createWorker(createManifest({ queries: [] }));
    await createTodoViaRpc("owner-a", "original-runtime");
    await createRealtimeOutboxEvent("event-1");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "event-1" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(response.status).toBe(200);
    expect(body).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
    expect(
      (
        deliveries[0] as {
          result: Array<{ text: string }>;
        }
      ).result.map((todo) => todo.text)
    ).toEqual(["original-runtime"]);
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
    const body = (await response.json()) as {
      error: { code: ErrorCode; message: string };
    };

    expect(response.status).toBe(400);
    expect(body.error).toEqual({
      code: ErrorCode.ValidationError,
      message: "Invalid function return value: Value must be a finite number",
    });
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

  it("retries multi-table conflicts without orphan or partial writes", async () => {
    const marker = "after-multi-table-conflict";

    const response = await invoke(
      "/api/mutation/todos:createTodoAndLabelWithOneTableConflict",
      {
        body: rpcBody({
          ownerToken: "owner-a",
          text: marker,
        }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      }
    );
    const body = (await response.json()) as { result: number };

    expect(response.status).toBe(200);
    expect(body.result).toBe(2);
    expect(multiTableConflictAttempts).toBe(2);
    await expect(
      countStoredDocuments("todos", "owner-a", marker)
    ).resolves.toBe(1);
    await expect(
      countStoredDocuments("labels", "owner-a", marker)
    ).resolves.toBe(1);
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

  it("returns validation errors for query unique duplicate results", async () => {
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

    expect(response.status).toBe(400);
    expect(body.error).toEqual({
      code: ErrorCode.ValidationError,
      message: 'Expected exactly one document from "todos", received 2',
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

  it("returns validation errors for mutation unique duplicate results", async () => {
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

    expect(response.status).toBe(400);
    expect(body.error).toEqual({
      code: ErrorCode.ValidationError,
      message: 'Expected exactly one document from "todos", received 2',
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

  it("writes from actions through runMutation", async () => {
    const before = await env.APP_DB.prepare(
      "SELECT version FROM _bf_table_versions WHERE table_name = 'todos'"
    ).first<{ version: number }>();

    const response = await invoke("/api/action/todos:writeViaMutation", {
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
