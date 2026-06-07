import {
  createExecutionContext,
  env,
  runDurableObjectAlarm,
  runInDurableObject,
  SELF,
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
import { RealtimeConnectionDO } from "./realtime/connection-do";
import { createRealtimeOutboxOperation } from "./realtime/outbox";
import {
  createRealtimeGlobalSubscriptionRouteTarget,
  createRealtimePartitionSubscriptionRouteTarget,
  createRealtimeTableSubscriptionRouteTarget,
  getRealtimeAffectedSubscriptionRouteTargets,
  getRealtimeConnectionShardName,
  getRealtimeSubscriptionShardName,
  getRealtimeSubscriptionShardNames,
  isZeroRealtimeVersionSnapshot,
} from "./realtime/routing";
import {
  evaluateRealtimeAutoscalingForTest,
  fetchRealtimeVersionSnapshot,
  REALTIME_PARTITION_VERSION_SNAPSHOT_BATCH_SIZE,
} from "./realtime/shards";
import {
  configuredRealtimeRuntimes,
  configureRealtimeRuntime,
  REALTIME_CONFIGURED_RUNTIME_LIMIT,
  resetRealtimeRuntimeStateForTest,
} from "./realtime/shared";
import { RealtimeSubscriptionDO } from "./realtime/subscription-do";
import {
  REALTIME_CATCH_UP_EVENT_LIMIT,
  REALTIME_CONNECTION_SHARD_COUNT,
  REALTIME_DELIVERY_BATCH_SIZE,
  REALTIME_MAX_RESTORE_SUBSCRIPTIONS,
  REALTIME_MAX_SUBSCRIPTION_SHARDS,
  REALTIME_OUTBOX_CLEANUP_INTERVAL_MS,
  REALTIME_PENDING_WORK_LIMIT,
  REALTIME_RUNTIME_EVICTIONS_METRIC,
  REALTIME_SCALE_DOWN_WINDOW_MS,
  REALTIME_SCALE_UP_WINDOW_MS,
} from "./realtime/types";
import { applyRuntimeSchema } from "./schema-apply";
import type {
  BaseflareManifest,
  BaseflareRuntimeEnv,
  D1Database,
  D1DatabaseSession,
  D1PreparedStatement,
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectStub,
} from "./types";

declare module "cloudflare:test" {
  interface ProvidedEnv {
    APP_DB: D1Database;
    REALTIME_CONNECTIONS: DurableObjectNamespace;
    REALTIME_SUBSCRIPTIONS: DurableObjectNamespace;
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

type AttachedTestWebSocket = WebSocket & {
  accept(): void;
  deserializeAttachment?: () => unknown;
  serializeAttachment?: (attachment: unknown) => void;
};

class FakeRealtimeDurableObjectState {
  readonly acceptedSockets: AttachedTestWebSocket[] = [];
  readonly attachments = new WeakMap<AttachedTestWebSocket, unknown>();
  private readonly hibernatedSockets: readonly AttachedTestWebSocket[];
  alarmTime: number | null = null;

  constructor(hibernatedSockets: readonly AttachedTestWebSocket[] = []) {
    this.hibernatedSockets = hibernatedSockets;
  }

  acceptWebSocket(socket: AttachedTestWebSocket): void {
    socket.serializeAttachment = (attachment: unknown) => {
      this.attachments.set(socket, attachment);
    };
    socket.deserializeAttachment = () => this.attachments.get(socket);
    this.acceptedSockets.push(socket);
    socket.accept?.();
  }

  getWebSockets(): AttachedTestWebSocket[] {
    return [...this.hibernatedSockets, ...this.acceptedSockets];
  }

  storage = {
    deleteAlarm: () => {
      this.alarmTime = null;
      return Promise.resolve();
    },
    getAlarm: () => Promise.resolve(this.alarmTime),
    setAlarm: (scheduledTime: number) => {
      this.alarmTime = scheduledTime;
      return Promise.resolve();
    },
  };
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
let realtimeDependencyTrackingQueryCalls = 0;
let realtimeDynamicDependencyOwnerToken = "owner-a";
let activeRealtimeConcurrencyQueries = 0;
let maxActiveRealtimeConcurrencyQueries = 0;

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

const realtimeDependencyTrackingQuery = query({
  args: {
    id: v.optional(v.id("todos")),
    mode: v.union(
      v.literal("broad-labels"),
      v.literal("dynamic-partition-todos"),
      v.literal("get-todo"),
      v.literal("partition-todos")
    ),
    ownerToken: v.string(),
  },
  handler(ctx, args) {
    realtimeDependencyTrackingQueryCalls += 1;
    if (args.mode === "broad-labels") {
      return ctx.db.query("labels").collect();
    }

    if (args.mode === "get-todo") {
      return args.id ? ctx.db.get("todos", args.id) : null;
    }

    const ownerToken =
      args.mode === "dynamic-partition-todos"
        ? realtimeDynamicDependencyOwnerToken
        : args.ownerToken;

    return ctx.db
      .query("todos")
      .filter({ ownerToken })
      .order("text", "asc")
      .collect();
  },
});

const realtimeNoReadQuery = query({
  args: { label: v.string() },
  handler(_ctx, args) {
    return [{ text: args.label }];
  },
});

const realtimeConcurrencyTrackingQuery = query({
  args: { ownerToken: v.string() },
  async handler(ctx, args) {
    activeRealtimeConcurrencyQueries += 1;
    maxActiveRealtimeConcurrencyQueries = Math.max(
      maxActiveRealtimeConcurrencyQueries,
      activeRealtimeConcurrencyQueries
    );
    await new Promise((resolve) => setTimeout(resolve, 10));
    activeRealtimeConcurrencyQueries -= 1;
    return ctx.db
      .query("todos")
      .filter({ ownerToken: args.ownerToken })
      .collect();
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
        definition: realtimeDependencyTrackingQuery,
        exportName: "dependencyTracking",
        modulePath: "realtime",
      },
      {
        definition: realtimeNoReadQuery,
        exportName: "noRead",
        modulePath: "realtime",
      },
      {
        definition: realtimeConcurrencyTrackingQuery,
        exportName: "concurrencyTracking",
        modulePath: "realtime",
      },
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

function createDeferred<T = void>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((settled) => {
    resolve = settled;
  });
  return { promise, resolve };
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

async function createRealtimeOutboxEvent(
  eventId: string,
  createdAt = Date.now(),
  options: {
    readonly bumpVersions?: boolean;
    readonly partitions?: readonly {
      readonly partitionKey: string;
      readonly partitionValue: string;
      readonly tableName: string;
    }[];
    readonly tables?: readonly string[];
  } = {}
): Promise<void> {
  await env.APP_DB.prepare(
    "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
  )
    .bind(
      eventId,
      createdAt,
      JSON.stringify(options.tables ?? ["todos"]),
      JSON.stringify(options.partitions ?? [])
    )
    .run();
  if (options.bumpVersions === false) {
    return;
  }

  const tables = options.tables ?? ["todos"];
  for (const tableName of tables) {
    await env.APP_DB.prepare(
      "UPDATE _bf_table_versions SET version = version + 1 WHERE table_name = ?"
    )
      .bind(tableName)
      .run();
  }
  for (const partition of options.partitions ?? []) {
    await env.APP_DB.prepare(
      "INSERT OR IGNORE INTO _bf_partition_versions (table_name, partition_key, partition_value, version) VALUES (?, ?, ?, 0)"
    )
      .bind(
        partition.tableName,
        partition.partitionKey,
        partition.partitionValue
      )
      .run();
    await env.APP_DB.prepare(
      "UPDATE _bf_partition_versions SET version = version + 1 WHERE table_name = ? AND partition_key = ? AND partition_value = ?"
    )
      .bind(
        partition.tableName,
        partition.partitionKey,
        partition.partitionValue
      )
      .run();
  }
}

function todoOwnerPartition(ownerToken: string): {
  readonly partitionKey: string;
  readonly partitionValue: string;
  readonly tableName: "todos";
} {
  return {
    partitionKey: "by_owner",
    partitionValue: JSON.stringify([ownerToken]),
    tableName: "todos",
  };
}

function todoOwnerPartitionId(ownerToken: string): string {
  const partition = todoOwnerPartition(ownerToken);
  return JSON.stringify([
    partition.tableName,
    partition.partitionKey,
    partition.partitionValue,
  ]);
}

function realtimeRegistrationKey(
  connectionKey: string,
  subscriptionId: string
): string {
  return JSON.stringify([connectionKey, subscriptionId]);
}

interface RealtimeIndexTestState {
  readonly registrationKeysByPartition: Map<string, Set<string>>;
  readonly registrationKeysByTable: Map<string, Set<string>>;
  readonly registrationKeysWithoutDependencies: Set<string>;
  readonly registrations: Map<
    string,
    { leaseExpiresAt: number; reEvaluationRetryAt?: number }
  >;
}

function getRealtimeIndexTestState(
  subscriptionDo: RealtimeSubscriptionDO
): RealtimeIndexTestState {
  return subscriptionDo as unknown as RealtimeIndexTestState;
}

async function getRealtimeOutboxSequence(eventId: string): Promise<number> {
  const row = await env.APP_DB.prepare(
    "SELECT sequence FROM _bf_realtime_outbox WHERE event_id = ?"
  )
    .bind(eventId)
    .first<{ sequence: number }>();
  if (!row) {
    throw new Error(`Missing realtime outbox event "${eventId}"`);
  }
  return row.sequence;
}

function getRealtimeDeliveryItems(delivery: unknown): Array<{
  connectionKey?: string;
  result: Array<{ text: string }>;
  sequence?: number | null;
  subscriptionId: string;
}> {
  const body = delivery as {
    connectionKey?: string;
    deliveries?: Array<{
      result: Array<{ text: string }>;
      sequence?: number | null;
      subscriptionId: string;
    }>;
    result?: Array<{ text: string }>;
    sequence?: number | null;
    subscriptionId?: string;
  };
  if (Array.isArray(body.deliveries)) {
    return body.deliveries.map((item) => ({
      ...item,
      connectionKey: body.connectionKey,
    }));
  }

  return [
    {
      connectionKey: body.connectionKey,
      result: body.result ?? [],
      sequence: body.sequence,
      subscriptionId: body.subscriptionId ?? "",
    },
  ];
}

function getFirstRealtimeDelivery(delivery: unknown): {
  connectionKey?: string;
  result: Array<{ text: string }>;
  sequence?: number | null;
  subscriptionId: string;
} {
  const [item] = getRealtimeDeliveryItems(delivery);
  if (!item) {
    throw new Error("Missing realtime delivery item");
  }

  return item;
}

async function acknowledgeRealtimeDeliveryRequest(
  request: Request
): Promise<Response> {
  const delivery = await request.json();
  const items = getRealtimeDeliveryItems(delivery);
  return Response.json({
    delivered: items.length,
    deliveredSubscriptions: items.map((item) => item.subscriptionId),
    ok: true,
  });
}

interface RealtimeClientMessage {
  message?: {
    result: Array<{ text: string }>;
    sequence: number | null;
    subscriptionId: string;
  };
  subscriptionId?: string;
  type: string;
}

async function waitFor(
  predicate: () => boolean,
  attempts = 200
): Promise<void> {
  for (let attempt = 0; attempt < attempts && !predicate(); attempt += 1) {
    await new Promise((resolve) => {
      setTimeout(resolve, 5);
    });
  }
}

function realtimeConnectionStub(clientId: string) {
  return env.REALTIME_CONNECTIONS.get(
    env.REALTIME_CONNECTIONS.idFromName(
      getRealtimeConnectionShardName(`client:${clientId}`)
    )
  );
}

// Opens a real WebSocket against the live Worker + Durable Objects and waits for
// the "subscribed" acknowledgement, returning the client and its message log.
async function openRealtimeClient(input: {
  clientId: string;
  ownerToken: string;
  subscriptionId: string;
}): Promise<{
  client: WebSocket & { accept?: () => void };
  messages: RealtimeClientMessage[];
}> {
  const response = await SELF.fetch(
    new Request(`http://example.com/api/subscribe?clientId=${input.clientId}`, {
      headers: {
        authorization: `Bearer ${input.ownerToken}`,
        upgrade: "websocket",
      },
      method: "GET",
    })
  );
  const client = (response as Response & { readonly webSocket?: WebSocket })
    .webSocket as WebSocket & { accept?: () => void };
  const messages: RealtimeClientMessage[] = [];
  client.accept?.();
  client.addEventListener("message", (event) => {
    messages.push(JSON.parse(String(event.data)) as RealtimeClientMessage);
  });
  client.send(
    JSON.stringify({
      args: { ownerToken: input.ownerToken },
      epoch: 1,
      queryName: "todos:list",
      subscriptionId: input.subscriptionId,
      type: "subscribe",
    })
  );
  await waitFor(() =>
    messages.some((message) => message.type === "subscribed")
  );
  return { client, messages };
}

async function createTodoViaSelf(
  ownerToken: string,
  text: string
): Promise<void> {
  const response = await SELF.fetch(
    new Request("http://example.com/api/mutation/todos:create", {
      body: rpcBody({ ownerToken, text }),
      headers: { authorization: `Bearer ${ownerToken}` },
      method: "POST",
    })
  );
  if (!response.ok) {
    throw new Error(`Mutation failed with status ${response.status}`);
  }
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
    activeRealtimeConcurrencyQueries = 0;
    maxActiveRealtimeConcurrencyQueries = 0;
    realtimeDependencyTrackingQueryCalls = 0;
    realtimeDynamicDependencyOwnerToken = "owner-a";
    rowConflictAttempts = 0;
    tableConflictAttempts = 0;
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.spyOn(console, "info").mockImplementation(() => undefined);
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
    await env.APP_DB.prepare("DELETE FROM _bf_realtime_shard_cursors").run();
    await env.APP_DB.prepare(
      "DELETE FROM _bf_realtime_shard_generations"
    ).run();
    await env.APP_DB.prepare(
      "INSERT INTO _bf_realtime_shard_generations (generation_id, subscription_shard_count, status, created_at, drain_after) VALUES (1, 1, 'active', 0, NULL)"
    ).run();
    await env.APP_DB.prepare(
      "UPDATE _bf_realtime_autoscale_state SET scale_up_started_at = NULL, scale_down_started_at = NULL, updated_at = 0 WHERE id = 1"
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

  it("rejects new realtime runtime configuration at the warning threshold", () => {
    const infoLog = vi.spyOn(console, "info").mockImplementation(() => {
      // Runtime metrics are asserted below.
    });
    const warnLog = vi.spyOn(console, "warn").mockImplementation(() => {
      // Runtime limit warnings are operator diagnostics.
    });
    resetRealtimeRuntimeStateForTest();
    let latestRuntimeId = "";
    for (let index = 0; index < REALTIME_CONFIGURED_RUNTIME_LIMIT; index += 1) {
      latestRuntimeId = createRealtimeRuntimeId();
    }

    expect(() => createRealtimeRuntimeId()).toThrow(
      `Realtime runtime configuration limit exceeded: ${REALTIME_CONFIGURED_RUNTIME_LIMIT}`
    );
    expect(configuredRealtimeRuntimes.size).toBe(
      REALTIME_CONFIGURED_RUNTIME_LIMIT
    );
    expect(configuredRealtimeRuntimes.has("runtime:1")).toBe(true);
    expect(configuredRealtimeRuntimes.has(latestRuntimeId)).toBe(true);
    expect(
      configuredRealtimeRuntimes.has(
        `runtime:${REALTIME_CONFIGURED_RUNTIME_LIMIT + 1}`
      )
    ).toBe(false);
    expect(warnLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_runtime_limit_exceeded",
        limit: REALTIME_CONFIGURED_RUNTIME_LIMIT,
        size: REALTIME_CONFIGURED_RUNTIME_LIMIT,
      })
    );
    expect(infoLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: REALTIME_RUNTIME_EVICTIONS_METRIC,
        tags: { result: "limit_exceeded" },
        value: 1,
      })
    );
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

  it("uses database time for realtime outbox timestamps", () => {
    const operation = createRealtimeOutboxOperation(
      {
        eventId: "db-time-event",
        partitions: [],
        tables: ["todos"],
      },
      1
    );

    expect(operation.params).toEqual([
      "db-time-event",
      JSON.stringify(["todos"]),
      JSON.stringify([]),
      1,
    ]);
    expect(operation.sql).toContain("julianday('now')");
  });

  it("applies realtime shard metadata", async () => {
    const generation = await env.APP_DB.prepare(
      "SELECT generation_id, subscription_shard_count, status FROM _bf_realtime_shard_generations WHERE generation_id = 1"
    ).first<{
      generation_id: number;
      status: string;
      subscription_shard_count: number;
    }>();
    const cursorTable = await env.APP_DB.prepare(
      "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '_bf_realtime_shard_cursors'"
    ).first<{ sql: string }>();
    const autoscaleState = await env.APP_DB.prepare(
      "SELECT id FROM _bf_realtime_autoscale_state WHERE id = 1"
    ).first<{ id: number }>();

    expect(generation).toEqual({
      generation_id: 1,
      status: "active",
      subscription_shard_count: 1,
    });
    expect(cursorTable?.sql).toContain("last_processed_outbox_sequence");
    expect(autoscaleState?.id).toBe(1);
  });

  it("batches realtime partition version snapshot reads", async () => {
    await env.APP_DB.prepare(
      "INSERT INTO _bf_partition_versions (table_name, partition_key, partition_value, version) VALUES (?, ?, ?, ?)"
    )
      .bind("todos", "by_owner", JSON.stringify(["owner-a"]), 7)
      .run();
    const preparedSql: string[] = [];
    const database: Pick<D1Database, "prepare"> = {
      prepare(sql) {
        preparedSql.push(sql);
        return env.APP_DB.prepare(sql);
      },
    };

    const snapshot = await fetchRealtimeVersionSnapshot(database, {
      partitions: new Set([
        todoOwnerPartitionId("owner-a"),
        todoOwnerPartitionId("owner-b"),
      ]),
      tables: new Set(["labels"]),
    });

    expect(snapshot.tables.get("labels")).toBe(0);
    expect(snapshot.partitions.get(todoOwnerPartitionId("owner-a"))).toBe(7);
    expect(snapshot.partitions.get(todoOwnerPartitionId("owner-b"))).toBe(0);
    expect(
      preparedSql.filter((sql) => sql.includes("_bf_partition_versions"))
    ).toHaveLength(1);
  });

  it("chunks large realtime partition version snapshots under the D1 bind limit", async () => {
    await env.APP_DB.prepare(
      "INSERT INTO _bf_partition_versions (table_name, partition_key, partition_value, version) VALUES (?, ?, ?, ?)"
    )
      .bind("todos", "by_owner", JSON.stringify(["owner-250"]), 11)
      .run();
    const preparedSql: string[] = [];
    const database: Pick<D1Database, "prepare"> = {
      prepare(sql) {
        preparedSql.push(sql);
        return env.APP_DB.prepare(sql);
      },
    };
    const partitionIds = Array.from({ length: 251 }, (_value, index) =>
      todoOwnerPartitionId(`owner-${index}`)
    );

    const snapshot = await fetchRealtimeVersionSnapshot(database, {
      partitions: new Set(partitionIds),
      tables: new Set(),
    });

    const partitionQueries = preparedSql.filter((sql) =>
      sql.includes("_bf_partition_versions")
    );
    expect(partitionQueries).toHaveLength(
      Math.ceil(
        partitionIds.length / REALTIME_PARTITION_VERSION_SNAPSHOT_BATCH_SIZE
      )
    );
    for (const sql of partitionQueries) {
      expect(sql.match(/\(\?, \?, \?, \?\)/g)?.length ?? 0).toBeLessThanOrEqual(
        REALTIME_PARTITION_VERSION_SNAPSHOT_BATCH_SIZE
      );
    }
    expect(snapshot.partitions.get(todoOwnerPartitionId("owner-0"))).toBe(0);
    expect(snapshot.partitions.get(todoOwnerPartitionId("owner-250"))).toBe(11);
  });

  it("classifies empty realtime version snapshots as unknown, not zero", () => {
    expect(
      isZeroRealtimeVersionSnapshot({
        partitions: new Map(),
        tables: new Map(),
      })
    ).toBe(false);
    expect(
      isZeroRealtimeVersionSnapshot({
        partitions: new Map([[todoOwnerPartitionId("owner-a"), 0]]),
        tables: new Map(),
      })
    ).toBe(true);
    expect(
      isZeroRealtimeVersionSnapshot({
        partitions: new Map(),
        tables: new Map([["todos", 1]]),
      })
    ).toBe(false);
  });

  it("scales realtime subscription generations geometrically", async () => {
    const startedAt = 1_000_000;
    await createRealtimeOutboxEvent("scale-up-high-water");
    const highWaterSequence = await getRealtimeOutboxSequence(
      "scale-up-high-water"
    );
    await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: 1000,
      now: startedAt,
    });
    const scaleUpResult = await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: 1000,
      now: startedAt + 10 * 60 * 1000,
    });
    const generationsAfterScaleUp = await env.APP_DB.prepare(
      "SELECT generation_id, subscription_shard_count, status FROM _bf_realtime_shard_generations ORDER BY generation_id"
    ).all<{
      generation_id: number;
      status: string;
      subscription_shard_count: number;
    }>();
    const cursorRowsAfterScaleUp = await env.APP_DB.prepare(
      "SELECT shard_name, last_processed_outbox_sequence FROM _bf_realtime_shard_cursors WHERE generation_id = 2 ORDER BY shard_name"
    ).all<{
      last_processed_outbox_sequence: number;
      shard_name: string;
    }>();

    expect(scaleUpResult).toBe("scaled_up");
    expect(generationsAfterScaleUp.results).toEqual([
      {
        generation_id: 1,
        status: "draining",
        subscription_shard_count: 1,
      },
      {
        generation_id: 2,
        status: "active",
        subscription_shard_count: 2,
      },
    ]);
    expect(cursorRowsAfterScaleUp.results).toEqual(
      getRealtimeSubscriptionShardNames({
        generationId: 2,
        subscriptionShardCount: 2,
      }).map((shardName) => ({
        last_processed_outbox_sequence: highWaterSequence,
        shard_name: shardName,
      }))
    );

    const lowLoadStartedAt = startedAt + 20 * 60 * 1000;
    await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: 0,
      now: lowLoadStartedAt,
    });
    const scaleDownResult = await evaluateRealtimeAutoscalingForTest(
      env.APP_DB,
      {
        activeRegistrationCount: 0,
        now: lowLoadStartedAt + 24 * 60 * 60 * 1000,
      }
    );
    const activeGeneration = await env.APP_DB.prepare(
      "SELECT generation_id, subscription_shard_count, status FROM _bf_realtime_shard_generations WHERE status = 'active'"
    ).first<{
      generation_id: number;
      status: string;
      subscription_shard_count: number;
    }>();

    expect(scaleDownResult).toBe("scaled_down");
    expect(activeGeneration).toEqual({
      generation_id: 3,
      status: "active",
      subscription_shard_count: 1,
    });

    await env.APP_DB.prepare(
      "DELETE FROM _bf_realtime_shard_generations"
    ).run();
    await env.APP_DB.prepare(
      "INSERT INTO _bf_realtime_shard_generations (generation_id, subscription_shard_count, status, created_at, drain_after) VALUES (1, 1, 'active', 0, NULL)"
    ).run();
    await env.APP_DB.prepare(
      "UPDATE _bf_realtime_autoscale_state SET scale_up_started_at = NULL, scale_down_started_at = NULL, updated_at = 0 WHERE id = 1"
    ).run();
  });

  it("treats concurrent realtime scale transitions as benign races", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const startedAt = 1_500_000;
    await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: 1000,
      now: startedAt,
    });
    await env.APP_DB.prepare(
      "INSERT INTO _bf_realtime_shard_generations (generation_id, subscription_shard_count, status, created_at, drain_after) VALUES (2, 1, 'retired', 0, NULL)"
    ).run();

    const result = await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: 1000,
      now: startedAt + 10 * 60 * 1000,
    });

    const activeGeneration = await env.APP_DB.prepare(
      "SELECT generation_id, subscription_shard_count, status FROM _bf_realtime_shard_generations WHERE status = 'active'"
    ).first<{
      generation_id: number;
      status: string;
      subscription_shard_count: number;
    }>();

    expect(activeGeneration).toEqual({
      generation_id: 1,
      status: "active",
      subscription_shard_count: 1,
    });
    expect(result).toBeNull();
    expect(
      info.mock.calls.some(
        ([, payload]) =>
          (payload as { metric?: string })?.metric ===
          "baseflare.runtime.realtime.autoscaling"
      )
    ).toBe(false);
  });

  it("initializes new realtime shard cursors to zero without outbox rows", async () => {
    const startedAt = 2_000_000;
    await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: 1000,
      now: startedAt,
    });

    await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: 1000,
      now: startedAt + 10 * 60 * 1000,
    });

    const cursorRows = await env.APP_DB.prepare(
      "SELECT shard_name, last_processed_outbox_sequence FROM _bf_realtime_shard_cursors WHERE generation_id = 2 ORDER BY shard_name"
    ).all<{
      last_processed_outbox_sequence: number;
      shard_name: string;
    }>();

    expect(cursorRows.results).toEqual(
      getRealtimeSubscriptionShardNames({
        generationId: 2,
        subscriptionShardCount: 2,
      }).map((shardName) => ({
        last_processed_outbox_sequence: 0,
        shard_name: shardName,
      }))
    );
  });

  it("never scales realtime subscription shards beyond the cap", async () => {
    // Start at the maximum generation size.
    await env.APP_DB.prepare(
      "DELETE FROM _bf_realtime_shard_generations"
    ).run();
    await env.APP_DB.prepare(
      `INSERT INTO _bf_realtime_shard_generations (generation_id, subscription_shard_count, status, created_at, drain_after) VALUES (1, ${REALTIME_MAX_SUBSCRIPTION_SHARDS}, 'active', 0, NULL)`
    ).run();

    // Sustained high load across the full scale-up window must NOT create a
    // larger generation once the v1 shard cap is reached.
    const startedAt = 2_000_000;
    const overCapRegistrations = REALTIME_MAX_SUBSCRIPTION_SHARDS * 2000;
    const first = await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: overCapRegistrations,
      now: startedAt,
    });
    const second = await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: overCapRegistrations,
      now: startedAt + 10 * 60 * 1000,
    });

    expect(first).toBeNull();
    expect(second).toBeNull();
    const generations = await env.APP_DB.prepare(
      "SELECT subscription_shard_count, status FROM _bf_realtime_shard_generations ORDER BY generation_id"
    ).all<{ status: string; subscription_shard_count: number }>();
    expect(generations.results).toEqual([
      {
        status: "active",
        subscription_shard_count: REALTIME_MAX_SUBSCRIPTION_SHARDS,
      },
    ]);
  });

  it("uses realtime pressure signals for autoscaling decisions", async () => {
    const startedAt = 2_000_000;
    await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: 10,
      now: startedAt,
      outboxLagMs: 60_000,
      pendingWorkCount: 1000,
    });
    const scaleUpResult = await evaluateRealtimeAutoscalingForTest(env.APP_DB, {
      activeRegistrationCount: 10,
      now: startedAt + 10 * 60 * 1000,
      outboxLagMs: 60_000,
      pendingWorkCount: 1000,
    });
    const activeGeneration = await env.APP_DB.prepare(
      "SELECT generation_id, subscription_shard_count, status FROM _bf_realtime_shard_generations WHERE status = 'active'"
    ).first<{
      generation_id: number;
      status: string;
      subscription_shard_count: number;
    }>();

    expect(scaleUpResult).toBe("scaled_up");
    expect(activeGeneration).toEqual({
      generation_id: 2,
      status: "active",
      subscription_shard_count: 2,
    });
  });

  it("starts realtime scale-down evaluation during empty catch-up", async () => {
    await env.APP_DB.prepare(
      "DELETE FROM _bf_realtime_shard_generations"
    ).run();
    await env.APP_DB.prepare(
      "INSERT INTO _bf_realtime_shard_generations (generation_id, subscription_shard_count, status, created_at, drain_after) VALUES (1, 2, 'active', 0, NULL)"
    ).run();
    const startedAt = 3_000_000;
    const now = vi.spyOn(Date, "now");
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const catchUp = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/catch-up", {
          body: JSON.stringify({
            afterSequence: null,
            shardName: "subscription:g1:0",
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    now.mockReturnValue(startedAt);
    await catchUp();
    now.mockReturnValue(startedAt + REALTIME_SCALE_DOWN_WINDOW_MS);
    await catchUp();

    const activeGeneration = await env.APP_DB.prepare(
      "SELECT generation_id, subscription_shard_count, status FROM _bf_realtime_shard_generations WHERE status = 'active'"
    ).first<{
      generation_id: number;
      status: string;
      subscription_shard_count: number;
    }>();

    expect(activeGeneration).toEqual({
      generation_id: 2,
      status: "active",
      subscription_shard_count: 1,
    });
  });

  it("evaluates realtime autoscaling when notify backpressure rejects work", async () => {
    const startedAt = 4_000_000;
    const now = vi.spyOn(Date, "now");
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const internals = subscriptionDo as unknown as {
      pendingNotifyEventIds: Set<string>;
    };
    for (let index = 0; index < REALTIME_PENDING_WORK_LIMIT; index += 1) {
      internals.pendingNotifyEventIds.add(`pending-${index}`);
    }
    const notify = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId: "backpressure-rejected" }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    now.mockReturnValue(startedAt);
    const firstResponse = await notify();
    const firstBody = (await firstResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    now.mockReturnValue(startedAt + REALTIME_SCALE_UP_WINDOW_MS);
    const secondResponse = await notify();
    const secondBody = (await secondResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    const activeGeneration = await env.APP_DB.prepare(
      "SELECT generation_id, subscription_shard_count, status FROM _bf_realtime_shard_generations WHERE status = 'active'"
    ).first<{
      generation_id: number;
      status: string;
      subscription_shard_count: number;
    }>();

    expect(activeGeneration).toEqual({
      generation_id: 2,
      status: "active",
      subscription_shard_count: 2,
    });
    expect(firstResponse.status).toBe(503);
    expect(firstBody).toEqual({ evaluated: 0, failed: 0, ok: false });
    expect(secondResponse.status).toBe(503);
    expect(secondBody).toEqual({ evaluated: 0, failed: 0, ok: false });
  });

  it("keeps duplicate realtime notify coalescing successful", async () => {
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const internals = subscriptionDo as unknown as {
      pendingNotifyEventIds: Set<string>;
    };
    internals.pendingNotifyEventIds.add("coalesced-event");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "coalesced-event" }),
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
    expect(body).toEqual({ evaluated: 0, failed: 0, ok: true });
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
      getRealtimeConnectionShardName("client:client-a")
    );
    expect(new URL(connections.requests[0]?.request.url ?? "").pathname).toBe(
      "/api/subscribe"
    );
  });

  it("delivers realtime updates through configured Durable Object bindings", async () => {
    createRealtimeRuntimeId();
    const response = await SELF.fetch(
      new Request("http://example.com/api/subscribe?clientId=real-do-client", {
        headers: {
          authorization: "Bearer owner-a",
          upgrade: "websocket",
        },
        method: "GET",
      })
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{
      message?: { result: Array<{ text: string }>; subscriptionId: string };
      subscriptionId?: string;
      type: string;
    }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as {
          message?: {
            result: Array<{ text: string }>;
            subscriptionId: string;
          };
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
        subscriptionId: "sub-real-do",
        type: "subscribe",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const mutationResponse = await SELF.fetch(
      new Request("http://example.com/api/mutation/todos:create", {
        body: rpcBody({ ownerToken: "owner-a", text: "real-do-delivery" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      })
    );
    expect(mutationResponse.ok).toBe(true);
    const outboxRow = await env.APP_DB.prepare(
      "SELECT event_id, sequence FROM _bf_realtime_outbox ORDER BY sequence DESC LIMIT 1"
    ).first<{ event_id: string; sequence: number }>();
    expect(outboxRow?.event_id).toEqual(expect.any(String));
    const catchUpResponse = await env.REALTIME_SUBSCRIPTIONS.get(
      env.REALTIME_SUBSCRIPTIONS.idFromName("subscription:g1:0")
    ).fetch("https://baseflare.internal/catch-up", {
      body: JSON.stringify({
        afterSequence: null,
        shardName: "subscription:g1:0",
      }),
      headers: { "content-type": "application/json" },
      method: "POST",
    });
    expect(catchUpResponse.ok).toBe(true);
    const catchUpResult = (await catchUpResponse.json()) as {
      evaluated: number;
      failed: number;
    };
    expect(catchUpResult).toMatchObject({ failed: 0 });
    for (
      let attempt = 0;
      attempt < 200 && !messages.some((message) => message.type === "delivery");
      attempt += 1
    ) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const delivery = messages.find((message) => message.type === "delivery");
    expect(messages[0]).toEqual({
      subscriptionId: "sub-real-do",
      type: "subscribed",
    });
    expect(delivery).toEqual({
      message: {
        result: expect.arrayContaining([
          expect.objectContaining({ text: "real-do-delivery" }),
        ]),
        sequence: outboxRow?.sequence,
        subscriptionId: "sub-real-do",
      },
      type: "delivery",
    });
  });

  // Real Durable Object coverage. workerd's test runtime cannot force genuine
  // hibernation *eviction*, so these exercise the real Hibernation API surface
  // (getWebSockets, serialize/deserialize attachments), real state.storage alarm
  // scheduling, and real cross-DO delivery. Actual eviction is staging-only
  // (pnpm test:perf:cloudflare).
  it("delivers realtime updates through a real reconciliation alarm", async () => {
    // Re-register the worker's realtime runtime (cleared by beforeEach) so the
    // subscription DO can resolve runtime:1 during re-evaluation.
    createRealtimeRuntimeId();
    const { messages } = await openRealtimeClient({
      clientId: "alarm-client",
      ownerToken: "owner-a",
      subscriptionId: "sub-alarm",
    });
    await createTodoViaSelf("owner-a", "alarm-delivered");

    // The notify fast path does not auto-deliver in the test env, so the only
    // way this reaches the socket is the real scheduled state.storage alarm.
    const ran = await runDurableObjectAlarm(
      realtimeConnectionStub("alarm-client")
    );
    expect(ran).toBe(true);

    await waitFor(() =>
      messages.some((message) => message.type === "delivery")
    );
    const delivery = messages.find((message) => message.type === "delivery");
    expect(delivery?.message).toEqual(
      expect.objectContaining({
        result: expect.arrayContaining([
          expect.objectContaining({ text: "alarm-delivered" }),
        ]),
        subscriptionId: "sub-alarm",
      })
    );
  });

  it("persists realtime socket attachments through the real Hibernation API", async () => {
    createRealtimeRuntimeId();
    const { messages } = await openRealtimeClient({
      clientId: "hibernation-client",
      ownerToken: "owner-a",
      subscriptionId: "sub-hib",
    });
    await createTodoViaSelf("owner-a", "hibernation-delivered");
    await runDurableObjectAlarm(realtimeConnectionStub("hibernation-client"));
    await waitFor(() =>
      messages.some((message) => message.type === "delivery")
    );
    const deliveredSequence =
      messages.find((message) => message.type === "delivery")?.message
        ?.sequence ?? null;
    expect(deliveredSequence).not.toBeNull();

    // Inspect the live Durable Object's real Hibernation API state: the socket
    // attachment must round-trip through serialize/deserialize with its
    // subscriptions and the latest delivered outbox sequence persisted.
    await runInDurableObject(
      realtimeConnectionStub("hibernation-client"),
      (_instance, state) => {
        interface StoredAttachment {
          connectionKey: string;
          latestDeliveredOutboxSequence: number | null;
          subscriptions: Array<{ queryName: string; subscriptionId: string }>;
        }
        // N=1 routes every client to the same connection shard, so locate this
        // client's socket by its persisted connection key.
        const sockets = state.getWebSockets() as Array<{
          deserializeAttachment(): unknown;
        }>;
        const attachment = sockets
          .map((socket) => socket.deserializeAttachment() as StoredAttachment)
          .find(
            (value) => value?.connectionKey === "client:hibernation-client"
          );
        expect(attachment).toBeDefined();
        expect(attachment?.subscriptions).toEqual([
          expect.objectContaining({
            queryName: "todos:list",
            subscriptionId: "sub-hib",
          }),
        ]);
        expect(attachment?.latestDeliveredOutboxSequence).toBe(
          deliveredSequence
        );
      }
    );
  });

  it("routes anonymous realtime connections with generated connection keys", async () => {
    const connections = new FakeDurableObjectNamespace();
    await invoke(
      "/api/subscribe",
      {
        headers: { upgrade: "websocket" },
        method: "GET",
      },
      worker,
      { ...env, REALTIME_CONNECTIONS: connections }
    );
    await invoke(
      "/api/subscribe",
      {
        headers: { upgrade: "websocket" },
        method: "GET",
      },
      worker,
      { ...env, REALTIME_CONNECTIONS: connections }
    );
    const firstKey = connections.requests[0]?.request.headers.get(
      "x-baseflare-realtime-connection-key"
    );
    const secondKey = connections.requests[1]?.request.headers.get(
      "x-baseflare-realtime-connection-key"
    );

    expect(firstKey).toMatch(/^anonymous:/);
    expect(secondKey).toMatch(/^anonymous:/);
    expect(firstKey).not.toBe(secondKey);
    expect(connections.requests.map((request) => request.name)).toEqual([
      getRealtimeConnectionShardName(firstKey ?? ""),
      getRealtimeConnectionShardName(secondKey ?? ""),
    ]);
  });

  it("keeps realtime connection shard routing deterministic and bounded", () => {
    const first = getRealtimeConnectionShardName("client-a");
    const second = getRealtimeConnectionShardName("client-a");
    const shardNumber = Number(first.split(":").at(1));
    const seenShardNames = new Set(
      Array.from({ length: 5000 }, (_value, index) =>
        getRealtimeConnectionShardName(`client-${index}`)
      )
    );

    expect(first).toBe(second);
    expect(first).toMatch(/^connection:\d+$/);
    expect(shardNumber).toBeGreaterThanOrEqual(0);
    expect(shardNumber).toBeLessThan(REALTIME_CONNECTION_SHARD_COUNT);
    expect(seenShardNames.size).toBe(REALTIME_CONNECTION_SHARD_COUNT);
  });

  it("keeps N=1 realtime subscription routes on the default generation shard", () => {
    expect(
      getRealtimeSubscriptionShardName(
        createRealtimeGlobalSubscriptionRouteTarget()
      )
    ).toBe("subscription:g1:0");
    expect(
      getRealtimeSubscriptionShardName(
        createRealtimeTableSubscriptionRouteTarget("todos")
      )
    ).toBe("subscription:g1:0");
    expect(
      getRealtimeSubscriptionShardName(
        createRealtimePartitionSubscriptionRouteTarget(
          todoOwnerPartition("owner-a")
        )
      )
    ).toBe("subscription:g1:0");
  });

  it("keeps future realtime subscription shard routing deterministic and bounded", () => {
    const shardCount = 32;
    const partitionRoute = createRealtimePartitionSubscriptionRouteTarget(
      todoOwnerPartition("owner-a")
    );
    const tableRoute = createRealtimeTableSubscriptionRouteTarget("todos");
    const first = getRealtimeSubscriptionShardName(partitionRoute, shardCount);
    const second = getRealtimeSubscriptionShardName(partitionRoute, shardCount);
    const tableShard = getRealtimeSubscriptionShardName(tableRoute, shardCount);
    const shardNumber = Number(first.split(":").at(2));

    expect(first).toBe(second);
    expect(first).toMatch(/^subscription:g1:\d+$/);
    expect(tableShard).toMatch(/^subscription:g1:\d+$/);
    expect(shardNumber).toBeGreaterThanOrEqual(0);
    expect(shardNumber).toBeLessThan(shardCount);
  });

  it("derives realtime subscription routes from affected tables and partitions", () => {
    const routes = getRealtimeAffectedSubscriptionRouteTargets({
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });

    expect(routes).toEqual([
      {
        type: "global",
      },
      {
        partition: todoOwnerPartition("owner-a"),
        type: "partition",
      },
      {
        tableName: "todos",
        type: "table",
      },
    ]);
  });

  const runRealtimeMoveRollbackScenario = async (
    unregisterHandler: () => Promise<Response>
  ): Promise<{
    readonly errorLog: ReturnType<typeof vi.spyOn>;
    readonly globalShardName: string;
    readonly registrations: Array<{ reEvaluationRetryAt?: number }>;
    readonly subscriptionDo: RealtimeSubscriptionDO;
    readonly subscriptionPaths: string[];
  }> => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    await env.APP_DB.prepare(
      "UPDATE _bf_realtime_shard_generations SET subscription_shard_count = 8 WHERE generation_id = 1"
    ).run();
    const globalShardName = getRealtimeSubscriptionShardName(
      createRealtimeGlobalSubscriptionRouteTarget(),
      8
    );
    const subscriptionPaths: string[] = [];
    const subscriptions = new FakeDurableObjectNamespace((_name, request) => {
      const pathname = new URL(request.url).pathname;
      subscriptionPaths.push(pathname);
      if (pathname === "/unregister") {
        return unregisterHandler();
      }

      return Promise.resolve(Response.json({ ok: true }));
    });
    const connections = new FakeDurableObjectNamespace((_name, request) => {
      const pathname = new URL(request.url).pathname;
      if (pathname === "/subscription-moved") {
        return Promise.resolve(new Response("failed", { status: 500 }));
      }

      return Promise.resolve(
        Response.json({
          delivered: 1,
          deliveredSubscriptions: ["sub-a"],
          ok: true,
        })
      );
    });
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: subscriptions,
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client:client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          shardName: globalShardName,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "owner-a-move-rollback");
    await createRealtimeOutboxEvent("owner-a-move-rollback", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "owner-a-move-rollback",
          shardName: globalShardName,
        }),
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
    const registrationsBody = (await registrationsResponse.json()) as {
      registrations: Array<{ reEvaluationRetryAt?: number }>;
    };

    return {
      errorLog,
      globalShardName,
      registrations: registrationsBody.registrations,
      subscriptionDo,
      subscriptionPaths,
    };
  };

  it("migrates partition-aligned realtime registrations to their home shard", async () => {
    const runtimeId = createRealtimeRuntimeId();
    await env.APP_DB.prepare(
      "UPDATE _bf_realtime_shard_generations SET subscription_shard_count = 8 WHERE generation_id = 1"
    ).run();
    const globalShardName = getRealtimeSubscriptionShardName(
      createRealtimeGlobalSubscriptionRouteTarget(),
      8
    );
    const partitionShardName = getRealtimeSubscriptionShardName(
      createRealtimePartitionSubscriptionRouteTarget(
        todoOwnerPartition("owner-a")
      ),
      8
    );
    const subscriptions = new FakeDurableObjectNamespace(() =>
      Promise.resolve(Response.json({ ok: true }))
    );
    const connections = new FakeDurableObjectNamespace((_name, request) => {
      const pathname = new URL(request.url).pathname;
      if (pathname === "/deliver") {
        return Promise.resolve(
          Response.json({
            delivered: 1,
            deliveredSubscriptions: ["sub-a"],
            ok: true,
          })
        );
      }

      return Promise.resolve(Response.json({ ok: true }));
    });
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: subscriptions,
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          shardName: globalShardName,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    await createTodoViaRpc("owner-a", "owner-a-sharded");
    await createRealtimeOutboxEvent("owner-a-sharded", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "owner-a-sharded",
          shardName: globalShardName,
        }),
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
    const registrationsBody = (await registrationsResponse.json()) as {
      registrations: unknown[];
    };

    expect(registrationsBody.registrations).toHaveLength(0);
    expect(
      subscriptions.requests.some((request) => {
        return (
          request.name === partitionShardName &&
          new URL(request.request.url).pathname === "/adopt-registration"
        );
      })
    ).toBe(true);
    expect(
      connections.requests.some((request) => {
        return new URL(request.request.url).pathname === "/subscription-moved";
      })
    ).toBe(true);
  });

  it("keeps source realtime registrations active when shard adoption fails", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    await env.APP_DB.prepare(
      "UPDATE _bf_realtime_shard_generations SET subscription_shard_count = 8 WHERE generation_id = 1"
    ).run();
    const globalShardName = getRealtimeSubscriptionShardName(
      createRealtimeGlobalSubscriptionRouteTarget(),
      8
    );
    const subscriptions = new FakeDurableObjectNamespace((_name, request) => {
      if (new URL(request.url).pathname === "/adopt-registration") {
        return Promise.resolve(new Response("failed", { status: 500 }));
      }

      return Promise.resolve(Response.json({ ok: true }));
    });
    const connections = new FakeDurableObjectNamespace(() =>
      Promise.resolve(
        Response.json({
          delivered: 1,
          deliveredSubscriptions: ["sub-a"],
          ok: true,
        })
      )
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: subscriptions,
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          shardName: globalShardName,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    await createTodoViaRpc("owner-a", "owner-a-adoption-failed");
    await createRealtimeOutboxEvent("owner-a-adoption-failed", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "owner-a-adoption-failed",
          shardName: globalShardName,
        }),
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
    const registrationsBody = (await registrationsResponse.json()) as {
      registrations: Array<{ reEvaluationRetryAt?: number }>;
    };

    expect(registrationsBody.registrations).toHaveLength(1);
    expect(
      registrationsBody.registrations[0]?.reEvaluationRetryAt
    ).toBeGreaterThan(Date.now());
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_registration_adoption_failed",
        shardName: expect.any(String),
        status: 500,
        subscriptionId: "sub-a",
      })
    );
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        errorName: "InternalRuntimeError",
        event: "runtime.realtime_registration_state_update_failed",
        subscriptionId: "sub-a",
      })
    );
  });

  it("keeps source realtime registrations active when shard move rollback succeeds", async () => {
    const { errorLog, registrations, subscriptionPaths } =
      await runRealtimeMoveRollbackScenario(() =>
        Promise.resolve(Response.json({ ok: true }))
      );

    expect(registrations).toHaveLength(1);
    expect(registrations[0]?.reEvaluationRetryAt).toBeGreaterThan(Date.now());
    expect(subscriptionPaths).toContain("/unregister");
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_registration_move_failed",
        status: 500,
        subscriptionId: "sub-a",
      })
    );
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        errorName: "InternalRuntimeError",
        event: "runtime.realtime_registration_state_update_failed",
        subscriptionId: "sub-a",
      })
    );
    expect(errorLog).not.toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_registration_move_cleanup_failed",
      })
    );
  });

  it("keeps source realtime registrations active when shard move rollback returns non-ok", async () => {
    const { errorLog, registrations } = await runRealtimeMoveRollbackScenario(
      () => Promise.resolve(Response.json({ ok: false }, { status: 503 }))
    );

    expect(registrations).toHaveLength(1);
    expect(registrations[0]?.reEvaluationRetryAt).toBeGreaterThan(Date.now());
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_registration_move_cleanup_failed",
        connectionKey: "client:client-a",
        sourceRemoved: false,
        status: 503,
        subscriptionId: "sub-a",
      })
    );
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_registration_move_failed",
        sourceRemoved: false,
        subscriptionId: "sub-a",
      })
    );
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        errorName: "InternalRuntimeError",
        event: "runtime.realtime_registration_state_update_failed",
        subscriptionId: "sub-a",
      })
    );
  });

  it("keeps source realtime registrations active when shard move rollback throws", async () => {
    const { errorLog, registrations } = await runRealtimeMoveRollbackScenario(
      () => Promise.reject(new Error("rollback unavailable"))
    );

    expect(registrations).toHaveLength(1);
    expect(registrations[0]?.reEvaluationRetryAt).toBeGreaterThan(Date.now());
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        errorName: "Error",
        event: "runtime.realtime_registration_move_cleanup_failed",
        connectionKey: "client:client-a",
        sourceRemoved: false,
        subscriptionId: "sub-a",
      })
    );
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_registration_move_failed",
        sourceRemoved: false,
        subscriptionId: "sub-a",
      })
    );
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        errorName: "InternalRuntimeError",
        event: "runtime.realtime_registration_state_update_failed",
        subscriptionId: "sub-a",
      })
    );
  });

  it("backs off realtime shard move retries after connection update failure", async () => {
    const { globalShardName, subscriptionDo, subscriptionPaths } =
      await runRealtimeMoveRollbackScenario(() =>
        Promise.resolve(Response.json({ ok: true }))
      );
    const adoptionAttemptsAfterFailure = subscriptionPaths.filter(
      (path) => path === "/adopt-registration"
    ).length;

    await createTodoViaRpc("owner-a", "owner-a-move-backed-off");
    await createRealtimeOutboxEvent("owner-a-move-backed-off", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "owner-a-move-backed-off",
          shardName: globalShardName,
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    expect(
      subscriptionPaths.filter((path) => path === "/adopt-registration")
    ).toHaveLength(adoptionAttemptsAfterFailure);

    const registration = getRealtimeIndexTestState(
      subscriptionDo
    ).registrations.get(realtimeRegistrationKey("client:client-a", "sub-a"));
    if (registration) {
      registration.reEvaluationRetryAt = Date.now() - 1;
    }
    await createTodoViaRpc("owner-a", "owner-a-move-retry");
    await createRealtimeOutboxEvent("owner-a-move-retry", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "owner-a-move-retry",
          shardName: globalShardName,
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(
      subscriptionPaths.filter((path) => path === "/adopt-registration")
    ).toHaveLength(adoptionAttemptsAfterFailure + 1);
  });

  // Migration delivery suppression guard: a migrated registration must not
  // re-deliver the result the source shard already delivered. Two mechanisms
  // make this safe: unchanged results are suppressed on re-evaluation, and
  // adopt-registration carries lastResultJson to the target.
  it("does not re-deliver an unchanged realtime result after a later notify", async () => {
    const runtimeId = createRealtimeRuntimeId();
    await createTodoViaRpc("owner-a", "stable");
    const deliveredIds: string[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const path = new URL(request.url).pathname;
        if (path === "/has-sockets") {
          return Response.json({ connected: true, ok: true });
        }
        if (path === "/deliver") {
          const body = (await request.json()) as {
            deliveries: Array<{ subscriptionId: string }>;
          };
          deliveredIds.push(
            ...body.deliveries.map((item) => item.subscriptionId)
          );
          return Response.json({
            delivered: body.deliveries.length,
            deliveredSubscriptions: body.deliveries.map(
              (item) => item.subscriptionId
            ),
            ok: true,
          });
        }
        return Response.json({ ok: true });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(() =>
        Promise.resolve(Response.json({ ok: true }))
      ),
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
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
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
          subscriptionId: "sub-expired",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const notify = (eventId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await createRealtimeOutboxEvent("ev1", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await notify("ev1");
    expect(deliveredIds.filter((id) => id === "sub-a")).toHaveLength(1);
    expect(deliveredIds.filter((id) => id === "sub-expired")).toHaveLength(1);
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const expiredRegistration = indexState.registrations.get(
      realtimeRegistrationKey("client-a", "sub-expired")
    );
    if (expiredRegistration) {
      expiredRegistration.leaseExpiresAt = Date.now() - 1;
    }

    // A later event bumps the version (forcing re-evaluation) but the query
    // result is unchanged — it must not be delivered a second time.
    await createRealtimeOutboxEvent("ev2", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await notify("ev2");
    expect(deliveredIds.filter((id) => id === "sub-a")).toHaveLength(1);
    expect(deliveredIds.filter((id) => id === "sub-expired")).toHaveLength(1);

    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      registrations: Array<{ subscriptionId: string }>;
    };
    expect(
      registrationsBody.registrations.map(
        (registration) => registration.subscriptionId
      )
    ).toEqual(["sub-a", "sub-expired"]);

    await createTodoViaRpc("owner-a", "changed-after-expired-unchanged");
    await createRealtimeOutboxEvent("ev3", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await notify("ev3");
    expect(deliveredIds.filter((id) => id === "sub-expired")).toHaveLength(2);

    const renewedRegistration = getRealtimeIndexTestState(
      subscriptionDo
    ).registrations.get(realtimeRegistrationKey("client-a", "sub-expired"));
    expect(renewedRegistration?.leaseExpiresAt).toBeGreaterThan(Date.now());
  });

  it("removes expired unchanged realtime registrations with no active sockets", async () => {
    const runtimeId = createRealtimeRuntimeId();
    await createTodoViaRpc("owner-a", "stable-disconnected");
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const path = new URL(request.url).pathname;
        if (path === "/deliver") {
          const body = (await request.json()) as {
            deliveries: Array<{ subscriptionId: string }>;
          };
          return Response.json({
            delivered: body.deliveries.length,
            deliveredSubscriptions: body.deliveries.map(
              (item) => item.subscriptionId
            ),
            ok: true,
          });
        }
        if (path === "/has-sockets") {
          return Response.json({ connected: false, ok: true });
        }
        return Response.json({ ok: true });
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
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const notify = (eventId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await createRealtimeOutboxEvent("stable-disconnected-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await notify("stable-disconnected-prime");
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
    const registration =
      getRealtimeIndexTestState(subscriptionDo).registrations.get(
        registrationKey
      );
    if (registration) {
      registration.leaseExpiresAt = Date.now() - 1;
    }

    await createRealtimeOutboxEvent("stable-disconnected-cleanup", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    const cleanupResponse = await notify("stable-disconnected-cleanup");
    const cleanupBody = (await cleanupResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(cleanupBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(
      getRealtimeIndexTestState(subscriptionDo).registrations.has(
        registrationKey
      )
    ).toBe(false);
  });

  it("deletes expired unchanged realtime registrations when liveness check fails", async () => {
    const runtimeId = createRealtimeRuntimeId();
    await createTodoViaRpc("owner-a", "stable-liveness-failure");
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const path = new URL(request.url).pathname;
        if (path === "/deliver") {
          const body = (await request.json()) as {
            deliveries: Array<{ subscriptionId: string }>;
          };
          return Response.json({
            delivered: body.deliveries.length,
            deliveredSubscriptions: body.deliveries.map(
              (item) => item.subscriptionId
            ),
            ok: true,
          });
        }
        if (path === "/has-sockets") {
          return Response.json({ ok: false }, { status: 503 });
        }
        return Response.json({ ok: true });
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
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const notify = (eventId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await createRealtimeOutboxEvent("stable-liveness-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await notify("stable-liveness-prime");
    const registration = getRealtimeIndexTestState(
      subscriptionDo
    ).registrations.get(realtimeRegistrationKey("client-a", "sub-a"));
    if (registration) {
      registration.leaseExpiresAt = Date.now() - 1;
    }

    await createRealtimeOutboxEvent("stable-liveness-failure", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    const failedResponse = await notify("stable-liveness-failure");
    const failedBody = (await failedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(failedBody).toEqual({ evaluated: 0, failed: 1, ok: true });
    expect(
      getRealtimeIndexTestState(subscriptionDo).registrations.has(
        realtimeRegistrationKey("client-a", "sub-a")
      )
    ).toBe(false);
  });

  it("preserves lastResultJson when adopting a migrated registration", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const lastResultJson = JSON.stringify([{ text: "already-delivered" }]);
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/adopt-registration", {
        body: JSON.stringify({
          args: { ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          lastResultJson,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "todos:list",
          runtimeId,
          shardName: "subscription:g1:0",
          subscriptionId: "sub-a",
        }),
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
      registrations: Array<{ lastResultJson?: string; subscriptionId: string }>;
    };

    expect(body.registrations).toEqual([
      expect.objectContaining({ lastResultJson, subscriptionId: "sub-a" }),
    ]);
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
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const responseB = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-b",
        {
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
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
          connectionKey: "client:client-a",
          deliveries: [
            {
              result: [{ text: "only-a" }],
              sequence: 123,
              subscriptionId: "sub-a",
            },
          ],
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
          sequence: 123,
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
          deliveries: [{ result: [], subscriptionId: "sub-missing" }],
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const missingBody = (await missingResponse.json()) as {
      delivered: number;
    };

    expect(missingBody.delivered).toBe(0);
    await expect(
      connectionDo.fetch(
        new Request("https://baseflare.internal/deliver", {
          body: JSON.stringify({
            connectionKey: "client:client-a",
            result: [{ text: "legacy-flat-payload" }],
            subscriptionId: "sub-legacy",
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      )
    ).rejects.toThrow('Realtime field "deliveries" must be an array');
  });

  it("delivers realtime messages as individual client messages", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const clientMessages: unknown[] = [];
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    client.accept?.();
    client.addEventListener("message", (event) => {
      clientMessages.push(JSON.parse(String(event.data)) as unknown);
    });

    const batchResponse = await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: "client:client-a",
          deliveries: [
            {
              result: [{ text: "first" }],
              sequence: 1,
              subscriptionId: "sub-a",
            },
            {
              result: [{ text: "second" }],
              sequence: 1,
              subscriptionId: "sub-b",
            },
          ],
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await batchResponse.json()) as {
      delivered: number;
      deliveredSubscriptions: string[];
    };
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(body.delivered).toBe(2);
    expect(body.deliveredSubscriptions).toEqual(["sub-a", "sub-b"]);
    expect(clientMessages).toEqual([
      {
        message: {
          result: [{ text: "first" }],
          sequence: 1,
          subscriptionId: "sub-a",
        },
        type: "delivery",
      },
      {
        message: {
          result: [{ text: "second" }],
          sequence: 1,
          subscriptionId: "sub-b",
        },
        type: "delivery",
      },
    ]);
  });

  it("reports only accepted subscriptions from realtime batch delivery", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const sentPayloads: string[] = [];
    const socket = {
      send(payload: string) {
        const message = JSON.parse(payload) as {
          message: { subscriptionId: string };
        };
        if (message.message.subscriptionId === "sub-b") {
          throw new Error("Socket closed");
        }

        sentPayloads.push(payload);
      },
    } as unknown as WebSocket;
    const internals = connectionDo as unknown as {
      socketStates: Map<WebSocket, { attachment: { connectionKey: string } }>;
      sockets: Set<WebSocket>;
      socketsByConnectionKey: Map<string, Set<WebSocket>>;
    };
    internals.sockets.add(socket);
    internals.socketStates.set(socket, {
      attachment: { connectionKey: "client-a" },
    });
    internals.socketsByConnectionKey.set("client-a", new Set([socket]));

    const response = await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: "client-a",
          deliveries: [
            { result: [], sequence: 1, subscriptionId: "sub-a" },
            { result: [], sequence: 1, subscriptionId: "sub-b" },
          ],
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      delivered: number;
      deliveredSubscriptions: string[];
      ok: boolean;
    };

    expect(body).toEqual({
      delivered: 1,
      deliveredSubscriptions: ["sub-a"],
      ok: true,
    });
    expect(sentPayloads).toHaveLength(1);
  });

  it("keeps realtime socket error reporting best-effort for closed sockets", () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const sentPayloads: string[] = [];
    const throwingSocket = {
      send() {
        throw new Error("Socket closed");
      },
    } as unknown as WebSocket;
    const activeSocket = {
      send(payload: string) {
        sentPayloads.push(payload);
      },
    } as unknown as WebSocket;
    const internals = connectionDo as unknown as {
      sendSocketError(socket: WebSocket, error: unknown): void;
    };

    expect(() =>
      internals.sendSocketError(throwingSocket, new Error("bad message"))
    ).not.toThrow();
    internals.sendSocketError(activeSocket, new Error("bad message"));

    expect(
      sentPayloads.map((payload) => JSON.parse(payload) as unknown)
    ).toEqual([{ error: "bad message", type: "error" }]);
  });

  it("does not expose placeholder realtime reconcile behavior", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });

    const response = await connectionDo.fetch(
      new Request("https://baseflare.internal/reconcile", { method: "POST" })
    );

    expect(response.status).toBe(404);
  });

  it("rejects direct realtime socket upgrades without a runtime id", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });

    await expect(
      connectionDo.fetch(
        new Request("https://baseflare.internal/api/subscribe", {
          headers: { upgrade: "websocket" },
          method: "GET",
        })
      )
    ).rejects.toThrow("runtime id");
    expect(state.acceptedSockets).toHaveLength(0);
  });

  it("isolates anonymous realtime sockets from each other", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const clientAMessages: unknown[] = [];
    const clientBMessages: unknown[] = [];
    const responseA = await connectionDo.fetch(
      new Request("https://baseflare.internal/api/subscribe", {
        headers: {
          upgrade: "websocket",
          "x-baseflare-realtime-runtime-id": "runtime:1",
        },
        method: "GET",
      })
    );
    const responseB = await connectionDo.fetch(
      new Request("https://baseflare.internal/api/subscribe", {
        headers: {
          upgrade: "websocket",
          "x-baseflare-realtime-runtime-id": "runtime:1",
        },
        method: "GET",
      })
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
    const internals = connectionDo as unknown as {
      socketStates: Map<WebSocket, { attachment: { connectionKey: string } }>;
    };
    const [clientAKey, clientBKey] = [...internals.socketStates.values()].map(
      ({ attachment }) => attachment.connectionKey
    );

    expect(clientAKey).toMatch(/^anonymous:/);
    expect(clientBKey).toMatch(/^anonymous:/);
    expect(clientAKey).not.toBe(clientBKey);

    const response = await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: clientAKey,
          deliveries: [
            { result: [{ text: "anonymous-a" }], subscriptionId: "sub-a" },
          ],
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as { delivered: number };
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(body.delivered).toBe(1);
    expect(clientAMessages).toHaveLength(1);
    expect(clientBMessages).toEqual([]);
  });

  it("closes malformed hibernated realtime socket attachments", async () => {
    const registerBodies: Array<{
      connectionKey: string;
      runtimeId: string;
      subscriptionId: string;
    }> = [];
    const closes = new Map<
      AttachedTestWebSocket,
      Array<{ code: number; reason: string }>
    >();
    const socketMessages = new Map<AttachedTestWebSocket, unknown[]>();
    const createMalformedSocket = (): AttachedTestWebSocket => {
      const messages: unknown[] = [];
      const closeCalls: Array<{ code: number; reason: string }> = [];
      const socket = {
        accept() {
          // Hibernated sockets are already accepted.
        },
        close(code: number, reason: string) {
          closeCalls.push({ code, reason });
        },
        deserializeAttachment() {
          return { malformed: true };
        },
        send(payload: string) {
          messages.push(JSON.parse(payload) as unknown);
        },
      } as AttachedTestWebSocket;
      socketMessages.set(socket, messages);
      closes.set(socket, closeCalls);
      return socket;
    };
    const socketA = createMalformedSocket();
    const socketB = createMalformedSocket();
    const connectionDo = new RealtimeConnectionDO(
      new FakeRealtimeDurableObjectState([socketA, socketB]),
      {
        APP_DB: env.APP_DB,
        REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
        REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
          async (_name, request) => {
            const path = new URL(request.url).pathname;
            if (path === "/register") {
              registerBodies.push(
                (await request.json()) as {
                  connectionKey: string;
                  runtimeId: string;
                  subscriptionId: string;
                }
              );
            }
            return Response.json({ ok: true });
          }
        ),
      }
    );
    const subscribe = (socket: AttachedTestWebSocket, subscriptionId: string) =>
      connectionDo.webSocketMessage(
        socket,
        JSON.stringify({
          args: { ownerToken: "owner-a" },
          epoch: 1,
          queryName: "todos:list",
          subscriptionId,
          type: "subscribe",
        })
      );

    expect(closes.get(socketA)).toEqual([
      { code: 1011, reason: "Session expired, please reconnect" },
    ]);
    expect(closes.get(socketB)).toEqual([
      { code: 1011, reason: "Session expired, please reconnect" },
    ]);

    await subscribe(socketA, "sub-a");
    await subscribe(socketB, "sub-b");

    expect(registerBodies).toEqual([]);
    expect(socketMessages.get(socketA)).toEqual([
      {
        error: "Realtime socket session expired",
        type: "error",
      },
    ]);
    expect(socketMessages.get(socketB)).toEqual([
      {
        error: "Realtime socket session expired",
        type: "error",
      },
    ]);
  });

  it("closes hibernated realtime socket attachments with empty runtime ids", async () => {
    const registerBodies: Array<{ runtimeId: string; subscriptionId: string }> =
      [];
    const closeCalls: Array<{ code: number; reason: string }> = [];
    const messages: unknown[] = [];
    const socket = {
      accept() {
        // Hibernated sockets are already accepted.
      },
      close(code: number, reason: string) {
        closeCalls.push({ code, reason });
      },
      deserializeAttachment() {
        return {
          connectionKey: "client-a",
          connectionName: getRealtimeConnectionShardName("client-a"),
          latestDeliveredOutboxSequence: null,
          runtimeId: "",
          subscriptions: [
            {
              args: { ownerToken: "owner-a" },
              epoch: 1,
              queryName: "todos:list",
              subscriptionId: "sub-empty-runtime",
            },
          ],
        };
      },
      send(payload: string) {
        messages.push(JSON.parse(payload) as unknown);
      },
    } as AttachedTestWebSocket;

    const connectionDo = new RealtimeConnectionDO(
      new FakeRealtimeDurableObjectState([socket]),
      {
        APP_DB: env.APP_DB,
        REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
        REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
          async (_name, request) => {
            if (new URL(request.url).pathname === "/register") {
              registerBodies.push(
                (await request.json()) as {
                  runtimeId: string;
                  subscriptionId: string;
                }
              );
            }

            return Response.json({ ok: true });
          }
        ),
      }
    );

    expect(closeCalls).toEqual([
      { code: 1011, reason: "Session expired, please reconnect" },
    ]);
    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 1,
        queryName: "todos:list",
        subscriptionId: "sub-empty-runtime",
        type: "subscribe",
      })
    );

    expect(registerBodies).toEqual([]);
    expect(messages).toEqual([
      {
        error: "Realtime socket session expired",
        type: "error",
      },
    ]);
  });

  it("closes hibernated realtime socket attachments with empty connection keys", async () => {
    const registerBodies: Array<{
      connectionKey: string;
      subscriptionId: string;
    }> = [];
    const closeCalls: Array<{ code: number; reason: string }> = [];
    const messages: unknown[] = [];
    const socket = {
      accept() {
        // Hibernated sockets are already accepted.
      },
      close(code: number, reason: string) {
        closeCalls.push({ code, reason });
      },
      deserializeAttachment() {
        return {
          connectionKey: "",
          connectionName: getRealtimeConnectionShardName("client-a"),
          latestDeliveredOutboxSequence: null,
          runtimeId: "runtime:1",
          subscriptions: [
            {
              args: { ownerToken: "owner-a" },
              epoch: 1,
              queryName: "todos:list",
              subscriptionId: "sub-empty-connection-key",
            },
          ],
        };
      },
      send(payload: string) {
        messages.push(JSON.parse(payload) as unknown);
      },
    } as AttachedTestWebSocket;

    const connectionDo = new RealtimeConnectionDO(
      new FakeRealtimeDurableObjectState([socket]),
      {
        APP_DB: env.APP_DB,
        REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
        REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
          async (_name, request) => {
            if (new URL(request.url).pathname === "/register") {
              registerBodies.push(
                (await request.json()) as {
                  connectionKey: string;
                  subscriptionId: string;
                }
              );
            }

            return Response.json({ ok: true });
          }
        ),
      }
    );

    expect(closeCalls).toEqual([
      { code: 1011, reason: "Session expired, please reconnect" },
    ]);
    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 1,
        queryName: "todos:list",
        subscriptionId: "sub-empty-connection-key",
        type: "subscribe",
      })
    );

    expect(registerBodies).toEqual([]);
    expect(messages).toEqual([
      {
        error: "Realtime socket session expired",
        type: "error",
      },
    ]);
  });

  it("drops malformed hibernated realtime socket subscriptions without closing the socket", async () => {
    const warnLog = vi.spyOn(console, "warn").mockImplementation(() => {
      // Malformed hibernated subscription entries are operator diagnostics.
    });
    const registerBodies: Array<{
      queryName: string;
      subscriptionId: string;
    }> = [];
    const closeCalls: Array<{ code: number; reason: string }> = [];
    const socket = {
      accept() {
        // Hibernated sockets are already accepted.
      },
      close(code: number, reason: string) {
        closeCalls.push({ code, reason });
      },
      deserializeAttachment() {
        return {
          connectionKey: "client-a",
          connectionName: getRealtimeConnectionShardName("client-a"),
          latestDeliveredOutboxSequence: null,
          runtimeId: "runtime:1",
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
        };
      },
      send() {
        // No client messages are expected for hibernation restore.
      },
    } as unknown as AttachedTestWebSocket;

    new RealtimeConnectionDO(new FakeRealtimeDurableObjectState([socket]), {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          if (new URL(request.url).pathname === "/register") {
            registerBodies.push(
              (await request.json()) as {
                queryName: string;
                subscriptionId: string;
              }
            );
          }

          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }
      ),
    });

    await waitFor(() => registerBodies.length === 1);

    expect(closeCalls).toEqual([]);
    expect(registerBodies).toEqual([
      expect.objectContaining({
        queryName: "todos:list",
        subscriptionId: "sub-good",
      }),
    ]);
    expect(warnLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        connectionKey: "client-a",
        event: "runtime.realtime_socket_subscription_attachment_dropped",
        runtimeId: "runtime:1",
        subscriptionIndex: 1,
      })
    );
  });

  it("keeps explicit realtime client ids grouped for delivery", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const clientAMessages: unknown[] = [];
    const clientBMessages: unknown[] = [];
    const connect = () =>
      connectionDo.fetch(
        new Request(
          "https://baseflare.internal/api/subscribe?clientId=shared-client",
          {
            headers: {
              upgrade: "websocket",
              "x-baseflare-realtime-runtime-id": "runtime:1",
            },
            method: "GET",
          }
        )
      );
    const responseA = await connect();
    const responseB = await connect();
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
          connectionKey: "client:shared-client",
          deliveries: [
            { result: [{ text: "shared" }], subscriptionId: "sub-shared" },
          ],
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as { delivered: number };
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(body.delivered).toBe(2);
    expect(clientAMessages).toHaveLength(1);
    expect(clientBMessages).toHaveLength(1);
  });

  it("keeps realtime client and session identifiers in separate connection namespaces", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const clientIdMessages: unknown[] = [];
    const sessionIdMessages: unknown[] = [];
    const openSocket = async (query: string) => {
      const response = await connectionDo.fetch(
        new Request(`https://baseflare.internal/api/subscribe?${query}`, {
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        })
      );
      const client = (response as Response & { readonly webSocket?: WebSocket })
        .webSocket as WebSocket & { accept?: () => void };
      client.accept?.();
      return client;
    };
    const clientIdSocket = await openSocket("clientId=same");
    const sessionIdSocket = await openSocket("sessionId=same");
    clientIdSocket.addEventListener("message", (event) => {
      clientIdMessages.push(JSON.parse(String(event.data)) as unknown);
    });
    sessionIdSocket.addEventListener("message", (event) => {
      sessionIdMessages.push(JSON.parse(String(event.data)) as unknown);
    });

    await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: "client:same",
          deliveries: [
            { result: [{ text: "client-only" }], subscriptionId: "sub-client" },
          ],
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: "session:same",
          deliveries: [
            {
              result: [{ text: "session-only" }],
              subscriptionId: "sub-session",
            },
          ],
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(clientIdMessages).toEqual([
      expect.objectContaining({
        message: expect.objectContaining({
          result: [{ text: "client-only" }],
          subscriptionId: "sub-client",
        }),
        type: "delivery",
      }),
    ]);
    expect(sessionIdMessages).toEqual([
      expect.objectContaining({
        message: expect.objectContaining({
          result: [{ text: "session-only" }],
          subscriptionId: "sub-session",
        }),
        type: "delivery",
      }),
    ]);
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
      socketStates: Map<
        WebSocket,
        {
          attachment: {
            authorizationHeader?: string;
            connectionKey: string;
            connectionName: string;
            latestDeliveredOutboxSequence: number | null;
            runtimeId: string;
            subscriptions: unknown[];
          };
        }
      >;
      sockets: Set<WebSocket>;
      socketsByConnectionKey: Map<string, Set<WebSocket>>;
    };
    internals.sockets.add(failedSocket);
    internals.sockets.add(activeSocket);
    internals.socketStates.set(failedSocket, {
      attachment: {
        authorizationHeader: "Bearer owner-a",
        connectionKey: "client-a",
        connectionName: "connection:0",
        latestDeliveredOutboxSequence: null,
        runtimeId: "runtime:1",
        subscriptions: [],
      },
    });
    internals.socketStates.set(activeSocket, {
      attachment: {
        connectionKey: "client-a",
        connectionName: "connection:0",
        latestDeliveredOutboxSequence: null,
        runtimeId: "runtime:1",
        subscriptions: [],
      },
    });
    internals.socketsByConnectionKey.set(
      "client-a",
      new Set([failedSocket, activeSocket])
    );

    const response = await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: "client-a",
          deliveries: [
            {
              result: [{ text: "still-delivered" }],
              subscriptionId: "sub-a",
            },
          ],
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
    expect(internals.socketStates.has(failedSocket)).toBe(false);
    expect(internals.socketsByConnectionKey.get("client-a")).toEqual(
      new Set([activeSocket])
    );
  });

  it("clears all realtime socket state after close", async () => {
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            authorization: "Bearer owner-a",
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const internals = connectionDo as unknown as {
      socketStates: Map<WebSocket, unknown>;
      sockets: Set<WebSocket>;
      socketsByConnectionKey: Map<string, Set<WebSocket>>;
    };
    client.accept?.();

    expect(internals.sockets.size).toBe(1);
    client.close();
    for (
      let attempt = 0;
      attempt < 20 && internals.sockets.size > 0;
      attempt += 1
    ) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(internals.sockets.size).toBe(0);
    expect(internals.socketStates.size).toBe(0);
    expect(internals.socketsByConnectionKey.size).toBe(0);
  });

  it("stores realtime socket attachments through the hibernation API", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const registerRequests: unknown[] = [];
    const connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          registerRequests.push(await request.json());
          return Response.json({ ok: true });
        }
      ),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            authorization: "Bearer owner-a",
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as AttachedTestWebSocket;
    client.accept?.();
    const [socket] = state.acceptedSockets;
    if (!socket) {
      throw new Error("Expected hibernated realtime socket");
    }

    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 1,
        queryName: "todos:list",
        subscriptionId: "sub-a",
        type: "subscribe",
      })
    );

    expect(registerRequests).toHaveLength(1);
    expect(state.alarmTime).toBeTypeOf("number");
    expect(state.attachments.get(socket)).toEqual(
      expect.objectContaining({
        authorizationHeader: "Bearer owner-a",
        connectionKey: "client:client-a",
        connectionName: getRealtimeConnectionShardName("client:client-a"),
        latestDeliveredOutboxSequence: null,
        runtimeId: "runtime:1",
        subscriptions: [
          expect.objectContaining({
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-a",
            subscriptionShardName: getRealtimeSubscriptionShardName(),
          }),
        ],
      })
    );
  });

  it("preserves a pending realtime reconciliation alarm during subscription activity", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(() =>
        Promise.resolve(Response.json({ ok: true }))
      ),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            authorization: "Bearer owner-a",
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as AttachedTestWebSocket;
    client.accept?.();
    const [socket] = state.acceptedSockets;
    if (!socket) {
      throw new Error("Expected accepted realtime socket");
    }

    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 1,
        queryName: "todos:list",
        subscriptionId: "sub-a",
        type: "subscribe",
      })
    );
    expect(state.alarmTime).toBeTypeOf("number");

    const pendingAlarm = 1_234_567;
    state.alarmTime = pendingAlarm;
    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 1,
        queryName: "todos:list",
        subscriptionId: "sub-b",
        type: "subscribe",
      })
    );
    expect(state.alarmTime).toBe(pendingAlarm);

    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({ subscriptionId: "sub-b", type: "unsubscribe" })
    );
    expect(state.alarmTime).toBe(pendingAlarm);

    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({ subscriptionId: "sub-a", type: "unsubscribe" })
    );
    expect(state.alarmTime).toBeNull();
  });

  it("restores hibernated realtime subscriptions and catches up from attachments", async () => {
    const state = new FakeRealtimeDurableObjectState();
    const subscriptionRequests: Array<{ body: unknown; path: string }> = [];
    let connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          subscriptionRequests.push({
            body: await request.json(),
            path: new URL(request.url).pathname,
          });
          return Response.json({ events: [], ok: true });
        }
      ),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            authorization: "Bearer owner-a",
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const [socket] = state.acceptedSockets;
    if (!socket) {
      throw new Error("Expected accepted realtime socket");
    }
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as AttachedTestWebSocket;
    client.accept?.();
    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 2,
        queryName: "todos:list",
        subscriptionId: "sub-a",
        type: "subscribe",
      })
    );
    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 3,
        queryName: "todos:list",
        subscriptionId: "sub-b",
        type: "subscribe",
      })
    );
    await connectionDo.fetch(
      new Request("https://baseflare.internal/deliver", {
        body: JSON.stringify({
          connectionKey: "client:client-a",
          deliveries: [{ result: [], sequence: 42, subscriptionId: "sub-a" }],
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    subscriptionRequests.length = 0;

    const prepare = vi.spyOn(env.APP_DB, "prepare");
    connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          subscriptionRequests.push({
            body: await request.json(),
            path: new URL(request.url).pathname,
          });
          return Response.json({ events: [], ok: true });
        }
      ),
    });
    await connectionDo.alarm();

    for (
      let attempt = 0;
      attempt < 20 && subscriptionRequests.length < 3;
      attempt += 1
    ) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(subscriptionRequests).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          body: expect.objectContaining({
            connectionKey: "client:client-a",
            epoch: 2,
            queryName: "todos:list",
            runtimeId: "runtime:1",
            subscriptionId: "sub-a",
          }),
          path: "/register",
        }),
        expect.objectContaining({
          body: expect.objectContaining({
            connectionKey: "client:client-a",
            epoch: 3,
            queryName: "todos:list",
            runtimeId: "runtime:1",
            subscriptionId: "sub-b",
          }),
          path: "/register",
        }),
        expect.objectContaining({
          body: expect.objectContaining({ afterSequence: 42 }),
          path: "/catch-up",
        }),
      ])
    );
    const generationQueries = prepare.mock.calls.filter(([sql]) =>
      sql.includes("_bf_realtime_shard_generations")
    );
    expect(generationQueries).toHaveLength(1);
  });

  it("logs and counts hibernation subscription restore failures", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const state = new FakeRealtimeDurableObjectState();
    const subscriptionRequests: Array<{ path: string }> = [];
    // First instance: subscribe so the hibernated attachment carries sub-a.
    let connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(() =>
        Promise.resolve(Response.json({ ok: true }))
      ),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            authorization: "Bearer owner-a",
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const [socket] = state.acceptedSockets;
    if (!socket) {
      throw new Error("Expected accepted realtime socket");
    }
    (
      response as Response & { readonly webSocket?: AttachedTestWebSocket }
    ).webSocket?.accept?.();
    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 1,
        queryName: "todos:list",
        subscriptionId: "sub-a",
        type: "subscribe",
      })
    );

    // Wake from hibernation against a subscription DO that rejects /register.
    state.alarmTime = null;
    let rejectRegisters = true;
    connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        (_name, request) => {
          const path = new URL(request.url).pathname;
          subscriptionRequests.push({ path });
          return Promise.resolve(
            path === "/register" && rejectRegisters
              ? Response.json({ ok: false }, { status: 503 })
              : Response.json({ events: [], ok: true })
          );
        }
      ),
    });

    // restoreHibernatedSockets fires restoreAttachedSubscriptions from the ctor.
    await waitFor(() =>
      errorLog.mock.calls.some(
        ([, payload]) =>
          (payload as { event?: string })?.event ===
          "runtime.realtime_hibernation_subscription_restore_failed"
      )
    );

    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_hibernation_subscription_restore_failed",
        subscriptionId: "sub-a",
      })
    );
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.restore_subscriptions",
        tags: { result: "rejected" },
        value: 1,
      })
    );
    expect(state.alarmTime).toBeTypeOf("number");

    state.alarmTime = null;
    await connectionDo.alarm();

    expect(state.alarmTime).toBeTypeOf("number");
    expect(
      subscriptionRequests.filter(({ path }) => path === "/register")
    ).toHaveLength(2);

    rejectRegisters = false;
    await connectionDo.alarm();

    expect(
      subscriptionRequests.filter(({ path }) => path === "/register")
    ).toHaveLength(3);
    expect(subscriptionRequests.some(({ path }) => path === "/catch-up")).toBe(
      true
    );
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.restore_subscriptions",
        tags: { result: "accepted" },
        value: 1,
      })
    );
  });

  it("catches up accepted hibernation restores when another restore still fails", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const state = new FakeRealtimeDurableObjectState();
    const subscriptionRequests: Array<{
      body: Record<string, unknown>;
      path: string;
    }> = [];
    let connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(() =>
        Promise.resolve(Response.json({ ok: true }))
      ),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            authorization: "Bearer owner-a",
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const [socket] = state.acceptedSockets;
    if (!socket) {
      throw new Error("Expected accepted realtime socket");
    }
    (
      response as Response & { readonly webSocket?: AttachedTestWebSocket }
    ).webSocket?.accept?.();
    for (const subscriptionId of ["sub-a", "sub-b"]) {
      await connectionDo.webSocketMessage(
        socket,
        JSON.stringify({
          args: { ownerToken: "owner-a" },
          epoch: subscriptionId === "sub-a" ? 1 : 2,
          queryName: "todos:list",
          subscriptionId,
          type: "subscribe",
        })
      );
    }

    connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          const body = (await request.json()) as Record<string, unknown>;
          const path = new URL(request.url).pathname;
          subscriptionRequests.push({ body, path });
          return path === "/register" && body.subscriptionId === "sub-b"
            ? Response.json({ ok: false }, { status: 503 })
            : Response.json({ events: [], ok: true });
        }
      ),
    });

    await waitFor(() =>
      errorLog.mock.calls.some(
        ([, payload]) =>
          (payload as { event?: string })?.event ===
          "runtime.realtime_hibernation_subscription_restore_failed"
      )
    );
    subscriptionRequests.length = 0;
    state.alarmTime = null;

    await connectionDo.alarm();

    expect(
      subscriptionRequests.filter(({ path }) => path === "/register")
    ).toEqual([
      expect.objectContaining({
        body: expect.objectContaining({ subscriptionId: "sub-a" }),
        path: "/register",
      }),
      expect.objectContaining({
        body: expect.objectContaining({ subscriptionId: "sub-b" }),
        path: "/register",
      }),
    ]);
    expect(subscriptionRequests.some(({ path }) => path === "/catch-up")).toBe(
      true
    );
    expect(state.alarmTime).toBeTypeOf("number");
  });

  it("retries hibernation restore when generation lookup fails at wakeup", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const state = new FakeRealtimeDurableObjectState();
    const subscriptionRequests: Array<{ path: string }> = [];
    let connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(() =>
        Promise.resolve(Response.json({ ok: true }))
      ),
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            authorization: "Bearer owner-a",
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const [socket] = state.acceptedSockets;
    if (!socket) {
      throw new Error("Expected accepted realtime socket");
    }
    (
      response as Response & { readonly webSocket?: AttachedTestWebSocket }
    ).webSocket?.accept?.();
    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({
        args: { ownerToken: "owner-a" },
        epoch: 1,
        queryName: "todos:list",
        subscriptionId: "sub-a",
        type: "subscribe",
      })
    );

    let rejectGenerationLookup = true;
    const database = {
      prepare(sql: string) {
        if (
          rejectGenerationLookup &&
          sql.includes("_bf_realtime_shard_generations")
        ) {
          rejectGenerationLookup = false;
          throw new Error("generation lookup unavailable");
        }

        return env.APP_DB.prepare(sql);
      },
    } as D1Database;
    state.alarmTime = null;
    connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: database,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        (_name, request) => {
          subscriptionRequests.push({ path: new URL(request.url).pathname });
          return Promise.resolve(
            Response.json({ evaluated: 0, events: [], failed: 0, ok: true })
          );
        }
      ),
    });

    await waitFor(() =>
      errorLog.mock.calls.some(
        ([, payload]) =>
          (payload as { event?: string })?.event ===
          "runtime.realtime_hibernation_restore_failed"
      )
    );

    expect(state.alarmTime).toBeTypeOf("number");
    expect(subscriptionRequests).toHaveLength(0);

    await connectionDo.alarm();

    expect(
      subscriptionRequests.filter(({ path }) => path === "/register")
    ).toHaveLength(1);
    expect(subscriptionRequests.some(({ path }) => path === "/catch-up")).toBe(
      true
    );
  });

  it("attempts realtime reconciliation catch-up on every shard after partial failures", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const catchUpShardNames: string[] = [];
    let failSecondShard = false;
    const socket = {
      accept() {
        // Hibernated sockets are already accepted.
      },
      deserializeAttachment() {
        return {
          connectionKey: "client:client-a",
          connectionName: getRealtimeConnectionShardName("client:client-a"),
          latestDeliveredOutboxSequence: 10,
          runtimeId: "runtime:1",
          subscriptions: [
            {
              args: { ownerToken: "owner-a" },
              epoch: 1,
              queryName: "todos:list",
              subscriptionId: "sub-a",
              subscriptionShardName: "subscription:g2:0",
            },
            {
              args: { ownerToken: "owner-a" },
              epoch: 1,
              queryName: "todos:list",
              subscriptionId: "sub-b",
              subscriptionShardName: "subscription:g2:1",
            },
          ],
        };
      },
      send() {
        // Reconciliation does not send direct client messages in this test.
      },
    } as unknown as AttachedTestWebSocket;
    const state = new FakeRealtimeDurableObjectState([socket]);
    const connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          const pathname = new URL(request.url).pathname;
          if (pathname === "/catch-up") {
            const body = (await request.json()) as { shardName: string };
            catchUpShardNames.push(body.shardName);
            if (failSecondShard && body.shardName === "subscription:g2:1") {
              return Response.json({ ok: false }, { status: 503 });
            }
          }

          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }
      ),
    });

    await waitFor(() => catchUpShardNames.length >= 2);
    catchUpShardNames.length = 0;
    failSecondShard = true;
    state.alarmTime = null;

    await connectionDo.alarm();

    expect(catchUpShardNames).toEqual([
      "subscription:g2:0",
      "subscription:g2:1",
    ]);
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_reconciliation_failed",
      })
    );
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.reconciliations",
        tags: { result: "failed" },
        value: 1,
      })
    );
    expect(state.alarmTime).toBeTypeOf("number");
  });

  it("restores realtime subscriptions without serial register round trips", async () => {
    let activeRegistrations = 0;
    let maxActiveRegistrations = 0;
    const registerRequests: unknown[] = [];
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/catch-up") {
          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }

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
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
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
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(registerRequests).toHaveLength(2);
    expect(maxActiveRegistrations).toBe(2);
    expect(messages.at(-1)).toEqual({
      failed: [],
      reconciled: true,
      type: "restored",
    });
  });

  it("restores the maximum allowed realtime subscription count", async () => {
    const registerRequests: unknown[] = [];
    let generationLookupCount = 0;
    const database = Object.create(env.APP_DB) as D1Database;
    database.prepare = (sql: string) => {
      if (sql.includes("_bf_realtime_shard_generations")) {
        generationLookupCount += 1;
      }
      return env.APP_DB.prepare(sql);
    };
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/catch-up") {
          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }

        registerRequests.push(await request.json());
        return Response.json({ ok: true });
      }
    );
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: database,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptionDo,
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{ type: string }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(JSON.parse(String(event.data)) as { type: string });
    });

    client.send(
      JSON.stringify({
        subscriptions: Array.from(
          { length: REALTIME_MAX_RESTORE_SUBSCRIPTIONS },
          (_value, index) => ({
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: `sub-${index}`,
          })
        ),
        type: "restore",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(registerRequests).toHaveLength(REALTIME_MAX_RESTORE_SUBSCRIPTIONS);
    expect(generationLookupCount).toBe(1);
    expect(messages.at(-1)).toMatchObject({ type: "restored" });
  });

  it("rejects oversized realtime restore payloads", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const subscriptionDo = new FakeDurableObjectNamespace();
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptionDo,
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
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

    client.send(
      JSON.stringify({
        subscriptions: Array.from(
          { length: REALTIME_MAX_RESTORE_SUBSCRIPTIONS + 1 },
          (_value, index) => ({
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: `sub-${index}`,
          })
        ),
        type: "restore",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(subscriptionDo.requests).toHaveLength(0);
    expect(messages).toEqual([
      {
        error: `Realtime restore can include at most ${REALTIME_MAX_RESTORE_SUBSCRIPTIONS} subscriptions to bound D1 concurrency and connection memory`,
        type: "error",
      },
    ]);
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.restore_subscriptions",
        tags: { result: "rejected" },
        value: REALTIME_MAX_RESTORE_SUBSCRIPTIONS + 1,
      })
    );
  });

  it("reports partial realtime restore failures after successful registrations", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const registerRequests: unknown[] = [];
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/catch-up") {
          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }

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
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{
      failed?: Array<{ error: string; index: number; subscriptionId?: string }>;
      reconciled?: boolean;
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
          reconciled?: boolean;
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
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(registerRequests).toHaveLength(1);
    expect(messages[0]).toEqual({
      failed: [
        {
          error: 'Realtime field "queryName" must be a non-empty string',
          index: 1,
          subscriptionId: "sub-bad",
        },
      ],
      reconciled: true,
      type: "restored",
    });
    // The restore metric must reflect only the registration that succeeded as
    // accepted, and report the failed registration as rejected.
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.restore_subscriptions",
        tags: { result: "accepted" },
        value: 1,
      })
    );
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.restore_subscriptions",
        tags: { result: "rejected" },
        value: 1,
      })
    );
  });

  it("reports realtime restore failures when registration returns an error response", async () => {
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/catch-up") {
          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }

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
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{
      failed?: Array<{ error: string; index: number; subscriptionId?: string }>;
      reconciled?: boolean;
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
          reconciled?: boolean;
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
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(messages).toEqual([
      {
        failed: [
          {
            error: "Realtime subscription registration failed with status 500",
            index: 1,
            subscriptionId: "sub-bad",
          },
        ],
        reconciled: true,
        type: "restored",
      },
    ]);
  });

  it("runs realtime restore catch-up from the supplied outbox sequence", async () => {
    let catchUpBody:
      | { afterSequence: number | null; outboxBookmark?: string }
      | undefined;
    const catchUpResponse = createDeferred<Response>();
    const restoreDatabase = {
      batch: env.APP_DB.batch.bind(env.APP_DB),
      prepare: env.APP_DB.prepare.bind(env.APP_DB),
      withSession: () => ({
        batch: env.APP_DB.batch.bind(env.APP_DB),
        getBookmark: () => "restore-catch-up-bookmark",
        prepare: env.APP_DB.prepare.bind(env.APP_DB),
      }),
    } as D1Database;
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/catch-up") {
          catchUpBody = (await request.json()) as {
            afterSequence: number | null;
            outboxBookmark?: string;
          };
          return await catchUpResponse.promise;
        }

        return Response.json({ ok: true });
      }
    );
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: restoreDatabase,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptionDo,
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{ reconciled?: boolean; type: string }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as { reconciled?: boolean; type: string }
      );
    });

    client.send(
      JSON.stringify({
        afterSequence: 42,
        subscriptions: [
          {
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-a",
          },
        ],
        type: "restore",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(catchUpBody).toEqual(
      expect.objectContaining({
        afterSequence: 42,
        outboxBookmark: "restore-catch-up-bookmark",
      })
    );
    expect(messages).toEqual([]);

    catchUpResponse.resolve(
      Response.json({ evaluated: 0, events: [], failed: 0, ok: true })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(messages.at(-1)).toEqual({
      failed: [],
      reconciled: true,
      type: "restored",
    });
  });

  it("uses conservative realtime catch-up when restore has no sequence", async () => {
    let catchUpBody: { afterSequence: number | null } | undefined;
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/catch-up") {
          catchUpBody = (await request.json()) as {
            afterSequence: number | null;
          };
          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }

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
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{ type: string }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(JSON.parse(String(event.data)) as { type: string });
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
        ],
        type: "restore",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 2; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(catchUpBody).toEqual(
      expect.objectContaining({ afterSequence: null })
    );
    expect(messages.at(-1)).toEqual({
      failed: [],
      reconciled: true,
      type: "restored",
    });
  });

  it("delivers current realtime results during restore catch-up from a stale sequence", async () => {
    const runtimeId = createRealtimeRuntimeId();
    let connectionDo!: RealtimeConnectionDO;
    let subscriptionDo!: RealtimeSubscriptionDO;
    const connections = new FakeDurableObjectNamespace((_name, request) =>
      connectionDo.fetch(request)
    );
    const subscriptions = new FakeDurableObjectNamespace((_name, request) =>
      subscriptionDo.fetch(request)
    );
    connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: subscriptions,
    });
    subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: subscriptions,
    });
    await createTodoViaRpc("owner-a", "restore-catch-up");
    await createRealtimeOutboxEvent("restore-stale-event", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    const sequence = await getRealtimeOutboxSequence("restore-stale-event");
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            authorization: "Bearer owner-a",
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": runtimeId,
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{
      message?: {
        result: Array<{ text: string }>;
        sequence: number | null;
        subscriptionId: string;
      };
      reconciled?: boolean;
      subscriptionId?: string;
      type: string;
    }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as {
          message?: {
            result: Array<{ text: string }>;
            sequence: number | null;
            subscriptionId: string;
          };
          reconciled?: boolean;
          subscriptionId?: string;
          type: string;
        }
      );
    });

    client.send(
      JSON.stringify({
        afterSequence: sequence - 1,
        subscriptions: [
          {
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-a",
          },
        ],
        type: "restore",
      })
    );
    for (let attempt = 0; attempt < 30 && messages.length < 2; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(messages).toEqual([
      {
        message: {
          result: expect.arrayContaining([
            expect.objectContaining({ text: "restore-catch-up" }),
          ]),
          sequence,
          subscriptionId: "sub-a",
        },
        type: "delivery",
      },
      { failed: [], reconciled: true, type: "restored" },
    ]);
  });

  it("reports realtime restore catch-up failures in the restored payload", async () => {
    const subscriptionDo = new FakeDurableObjectNamespace((_name, request) => {
      if (new URL(request.url).pathname === "/catch-up") {
        return Promise.resolve(Response.json({ ok: false }, { status: 500 }));
      }

      return Promise.resolve(Response.json({ ok: true }));
    });
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptionDo,
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{
      failed?: Array<{ error: string; index: number }>;
      subscriptionId?: string;
      reconciled?: boolean;
      type: string;
    }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as {
          failed?: Array<{ error: string; index: number }>;
          subscriptionId?: string;
          reconciled?: boolean;
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
            subscriptionId: "sub-a",
          },
        ],
        type: "restore",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    expect(messages).toEqual([
      {
        failed: [
          {
            error: `Realtime restore catch-up failed for shard ${getRealtimeSubscriptionShardName()} with status 500`,
            index: -1,
          },
        ],
        reconciled: false,
        type: "restored",
      },
    ]);
  });

  it("reports partial realtime restore catch-up failures per shard", async () => {
    const catchUpShardNames: string[] = [];
    const subscriptionDo = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const pathname = new URL(request.url).pathname;
        if (pathname === "/catch-up") {
          const body = (await request.json()) as { shardName: string };
          catchUpShardNames.push(body.shardName);
          if (body.shardName === "subscription:g2:1") {
            return Response.json({ ok: false }, { status: 503 });
          }

          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }

        return Response.json({ ok: true });
      }
    );
    let attachment: unknown = {
      connectionKey: "client-a",
      connectionName: getRealtimeConnectionShardName("client-a"),
      latestDeliveredOutboxSequence: null,
      runtimeId: "runtime:1",
      subscriptions: [
        {
          args: { ownerToken: "owner-a" },
          epoch: 1,
          queryName: "todos:list",
          subscriptionId: "sub-a",
          subscriptionShardName: "subscription:g2:0",
        },
        {
          args: { ownerToken: "owner-a" },
          epoch: 1,
          queryName: "todos:list",
          subscriptionId: "sub-b",
          subscriptionShardName: "subscription:g2:1",
        },
      ],
    };
    const messages: Array<{
      failed?: Array<{ error: string; index: number }>;
      reconciled?: boolean;
      type: string;
    }> = [];
    const socket = {
      accept() {
        // Test socket accept is a no-op.
      },
      deserializeAttachment() {
        return attachment;
      },
      send(payload: string) {
        messages.push(JSON.parse(payload) as { type: string });
      },
      serializeAttachment(nextAttachment: unknown) {
        attachment = nextAttachment;
      },
    } as AttachedTestWebSocket;
    const state = new FakeRealtimeDurableObjectState([socket]);
    const connectionDo = new RealtimeConnectionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptionDo,
    });
    for (
      let attempt = 0;
      attempt < 20 && catchUpShardNames.length < 2;
      attempt += 1
    ) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    catchUpShardNames.length = 0;
    messages.length = 0;

    await connectionDo.webSocketMessage(
      socket,
      JSON.stringify({ subscriptions: [], type: "restore" })
    );

    expect(catchUpShardNames).toEqual([
      "subscription:g2:0",
      "subscription:g2:1",
    ]);
    expect(messages.at(-1)).toEqual({
      failed: [
        {
          error:
            "Realtime restore catch-up failed for shard subscription:g2:1 with status 503",
          index: -1,
        },
      ],
      reconciled: false,
      type: "restored",
    });
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
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
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
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
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

  it("targets realtime unsubscribe to the moved subscription shard", async () => {
    const subscriptions = new FakeDurableObjectNamespace(() =>
      Promise.resolve(Response.json({ ok: true }))
    );
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptions,
    });
    const response = await connectionDo.fetch(
      new Request(
        "https://baseflare.internal/api/subscribe?clientId=client-a",
        {
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
          method: "GET",
        }
      )
    );
    const client = (response as Response & { readonly webSocket?: WebSocket })
      .webSocket as WebSocket & { accept?: () => void };
    const messages: Array<{ subscriptionId?: string; type: string }> = [];
    client.accept?.();
    client.addEventListener("message", (event) => {
      messages.push(
        JSON.parse(String(event.data)) as {
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
        subscriptionId: "sub-a",
        type: "subscribe",
      })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    await connectionDo.fetch(
      new Request("https://baseflare.internal/subscription-moved", {
        body: JSON.stringify({
          connectionKey: "client:client-a",
          subscriptionId: "sub-a",
          subscriptionShardName: "subscription:g2:7",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    client.send(
      JSON.stringify({ subscriptionId: "sub-a", type: "unsubscribe" })
    );
    for (let attempt = 0; attempt < 20 && messages.length < 2; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const unregisterRequest = subscriptions.requests.find((request) => {
      return new URL(request.request.url).pathname === "/unregister";
    });
    expect(unregisterRequest?.name).toBe("subscription:g2:7");
    expect(messages.at(-1)).toEqual({
      subscriptionId: "sub-a",
      type: "unsubscribed",
    });
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
          headers: {
            upgrade: "websocket",
            "x-baseflare-realtime-runtime-id": "runtime:1",
          },
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
        const delivery = await request.json();
        deliveries.push(delivery);
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
    const sequence = await getRealtimeOutboxSequence("event-1");

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
    const delivery = getFirstRealtimeDelivery(deliveries[0]);
    expect(delivery.subscriptionId).toBe("sub-good");
    expect(delivery.sequence).toBe(sequence);
    expect(delivery.result.map((todo) => todo.text)).toEqual([
      "still-delivered",
    ]);
    expect(errorSpy).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_registration_re_evaluation_failed",
        queryName: "todos:renamed",
        subscriptionId: "sub-bad",
      })
    );
    const registrationKey = realtimeRegistrationKey("client-a", "sub-bad");
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const failedRegistration = indexState.registrations.get(registrationKey);
    expect(failedRegistration?.reEvaluationRetryAt).toBeGreaterThan(Date.now());

    await createTodoViaRpc("owner-a", "skips-backed-off-registration");
    await createRealtimeOutboxEvent("event-2");
    const skippedResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "event-2" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const skippedBody = (await skippedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    expect(skippedBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(errorSpy).toHaveBeenCalledTimes(1);

    if (failedRegistration) {
      failedRegistration.reEvaluationRetryAt = Date.now() - 1;
    }
    await createTodoViaRpc("owner-a", "retries-after-backoff");
    await createRealtimeOutboxEvent("event-3");
    const retriedResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "event-3" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const retriedBody = (await retriedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    expect(retriedBody).toEqual({ evaluated: 1, failed: 1, ok: true });
    expect(errorSpy).toHaveBeenCalledTimes(2);
  });

  it("removes expired realtime registrations after query evaluation failures", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
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
          queryName: "todos:renamed",
          runtimeId,
          subscriptionId: "sub-expired",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "expired-query-failure");
    await createRealtimeOutboxEvent("expired-query-failure");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "expired-query-failure" }),
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

  it("retries transient realtime shard notification failures", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    let notifyAttempts = 0;
    const subscriptions = new FakeDurableObjectNamespace((_name, request) => {
      if (new URL(request.url).pathname === "/notify") {
        notifyAttempts += 1;
        return Promise.resolve(
          notifyAttempts === 1
            ? Response.json({ ok: false }, { status: 503 })
            : Response.json({ ok: true })
        );
      }

      return Promise.resolve(Response.json({ ok: true }));
    });

    await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "retry-notify" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, REALTIME_SUBSCRIPTIONS: subscriptions }
    );
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(notifyAttempts).toBe(2);
    expect(
      errorLog.mock.calls.some(
        ([, payload]) =>
          (payload as { event?: string })?.event ===
          "runtime.realtime_notify_failed"
      )
    ).toBe(false);
  });

  it("retries realtime shard notification backpressure rejection", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const internals = subscriptionDo as unknown as {
      pendingNotifyEventIds: Set<string>;
    };
    let notifyAttempts = 0;
    const subscriptions = new FakeDurableObjectNamespace((_name, request) => {
      if (new URL(request.url).pathname !== "/notify") {
        return Promise.resolve(Response.json({ ok: true }));
      }

      notifyAttempts += 1;
      if (notifyAttempts === 1) {
        for (let index = 0; index < REALTIME_PENDING_WORK_LIMIT; index += 1) {
          internals.pendingNotifyEventIds.add(`pending-${index}`);
        }
      } else {
        internals.pendingNotifyEventIds.clear();
      }

      return subscriptionDo.fetch(request);
    });

    await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "retry-backpressure" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, REALTIME_SUBSCRIPTIONS: subscriptions }
    );
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(notifyAttempts).toBe(2);
    expect(
      errorLog.mock.calls.some(
        ([, payload]) =>
          (payload as { event?: string })?.event ===
          "runtime.realtime_notify_failed"
      )
    ).toBe(false);
  });

  it("recovers exhausted 429 realtime shard notification with immediate catch-up", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const bookmark = "catch-up-bookmark";
    const session: D1DatabaseSession = {
      batch: (statements) => env.APP_DB.batch(statements),
      getBookmark: () => bookmark,
      prepare: (sql) => env.APP_DB.prepare(sql),
    };
    const database: D1Database = {
      batch: (statements) => env.APP_DB.batch(statements),
      prepare: (sql) => env.APP_DB.prepare(sql),
      withSession: () => session,
    };
    let eventId: string | undefined;
    let notifyAttempts = 0;
    const catchUpBodies: Array<{
      afterSequence: number | null;
      outboxBookmark?: string | null;
      shardName: string;
    }> = [];
    const subscriptions = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const path = new URL(request.url).pathname;
        if (path === "/notify") {
          notifyAttempts += 1;
          const body = (await request.json()) as { eventId: string };
          eventId = body.eventId;
          return Response.json({ ok: false }, { status: 429 });
        }

        if (path === "/catch-up") {
          catchUpBodies.push(
            (await request.json()) as {
              afterSequence: number | null;
              outboxBookmark?: string | null;
              shardName: string;
            }
          );
          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }

        return Response.json({ ok: true });
      }
    );

    await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "recover-notify" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, APP_DB: database, REALTIME_SUBSCRIPTIONS: subscriptions }
    );
    await new Promise((resolve) => setTimeout(resolve, 25));

    if (!eventId) {
      throw new Error("Expected realtime notify event id");
    }

    const sequence = await getRealtimeOutboxSequence(eventId);
    expect(notifyAttempts).toBe(2);
    expect(catchUpBodies).toEqual([
      expect.objectContaining({
        afterSequence: sequence - 1,
        outboxBookmark: bookmark,
        shardName: getRealtimeSubscriptionShardName(),
      }),
    ]);
    expect(
      errorLog.mock.calls.some(
        ([, payload]) =>
          (payload as { event?: string })?.event ===
          "runtime.realtime_notify_failed"
      )
    ).toBe(false);
  });

  it("logs realtime notify failure when immediate catch-up recovery fails", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    let notifyAttempts = 0;
    let catchUpAttempts = 0;
    const subscriptions = new FakeDurableObjectNamespace((_name, request) => {
      const path = new URL(request.url).pathname;
      if (path === "/notify") {
        notifyAttempts += 1;
        return Promise.resolve(Response.json({ ok: false }, { status: 503 }));
      }

      if (path === "/catch-up") {
        catchUpAttempts += 1;
        return Promise.reject(new TypeError("catch-up transport unavailable"));
      }

      return Promise.resolve(Response.json({ ok: true }));
    });

    await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "failed-recovery" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, REALTIME_SUBSCRIPTIONS: subscriptions }
    );
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(notifyAttempts).toBe(2);
    expect(catchUpAttempts).toBe(1);
    expect(
      errorLog.mock.calls.some(([, payload]) => {
        const event = payload as {
          errorName?: string;
          event?: string;
          shardName?: string;
        };
        return (
          event.event === "runtime.realtime_notify_failed" &&
          event.errorName === "TypeError" &&
          event.shardName === undefined
        );
      })
    ).toBe(true);
  });

  it("does not retry non-retryable realtime shard notification failures", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    let notifyAttempts = 0;
    let catchUpAttempts = 0;
    const subscriptions = new FakeDurableObjectNamespace((_name, request) => {
      const path = new URL(request.url).pathname;
      if (path === "/notify") {
        notifyAttempts += 1;
        return Promise.resolve(Response.json({ ok: false }, { status: 400 }));
      }

      if (path === "/catch-up") {
        catchUpAttempts += 1;
      }

      return Promise.resolve(Response.json({ ok: true }));
    });

    await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "bad-notify" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      { ...env, REALTIME_SUBSCRIPTIONS: subscriptions }
    );
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(notifyAttempts).toBe(1);
    expect(catchUpAttempts).toBe(0);
    expect(
      errorLog.mock.calls.some(
        ([, payload]) =>
          (payload as { event?: string })?.event ===
          "runtime.realtime_notify_failed"
      )
    ).toBe(true);
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
      lastProcessedOutboxSequence: number | null;
    };

    expect(registrationsBody.lastProcessedOutboxSequence).toBe(
      body.events[0]?.sequence
    );
  });

  it("uses realtime catch-up bookmarks for outbox event reads and evaluation", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const sessionConstraints: string[] = [];
    const sessionPreparedSql: string[] = [];
    const staleOutboxStatement = {
      all: async () => ({ results: [] }),
      bind() {
        return this;
      },
      first: async () => null,
      run: async () => ({ meta: {}, success: true }),
    } as unknown as D1PreparedStatement;
    const session: D1DatabaseSession = {
      batch: (statements) => env.APP_DB.batch(statements),
      getBookmark: () => "session-bookmark",
      prepare: (sql) => {
        sessionPreparedSql.push(sql);
        return env.APP_DB.prepare(sql);
      },
    };
    const database: D1Database = {
      batch: (statements) => env.APP_DB.batch(statements),
      prepare: (sql) => {
        if (
          sql.includes("FROM _bf_realtime_outbox") &&
          sql.includes("ORDER BY sequence ASC LIMIT")
        ) {
          return staleOutboxStatement;
        }

        return env.APP_DB.prepare(sql);
      },
      withSession: (constraint) => {
        sessionConstraints.push(constraint ?? "");
        return session;
      },
    };
    await createTodoViaRpc("owner-a", "bookmark-catch-up");
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: database,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          const delivery = await request.json();
          deliveries.push(delivery);
          const items = getRealtimeDeliveryItems(delivery);
          return Response.json({
            delivered: items.length,
            deliveredSubscriptions: items.map((item) => item.subscriptionId),
            ok: true,
          });
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
          subscriptionId: "sub-catch-up-bookmark",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({
          afterSequence: null,
          limit: 10,
          outboxBookmark: "commit-bookmark",
        }),
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

    expect(response.status).toBe(200);
    expect(body).toEqual(
      expect.objectContaining({
        evaluated: 1,
        failed: 0,
        ok: true,
      })
    );
    expect(body.events).toHaveLength(1);
    expect(deliveries).toHaveLength(1);
    expect(sessionConstraints).toEqual(["commit-bookmark"]);
    expect(
      sessionPreparedSql.some((sql) => sql.includes("FROM _bf_realtime_outbox"))
    ).toBe(true);
    expect(sessionPreparedSql.some((sql) => sql.includes("FROM todos"))).toBe(
      true
    );
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

  it("fully re-evaluates realtime subscriptions when catch-up history has a gap", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const prepare = vi.spyOn(env.APP_DB, "prepare");
    await createTodoViaRpc("owner-a", "gap-recovered");
    await createRealtimeOutboxEvent("gap-deleted", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await createRealtimeOutboxEvent("gap-retained", Date.now(), {
      partitions: [todoOwnerPartition("owner-b")],
      tables: ["todos"],
    });
    await env.APP_DB.prepare(
      "DELETE FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("gap-deleted")
      .run();
    const deliveries: unknown[] = [];
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          deliveries.push(await request.json());
          return Promise.resolve(
            Response.json({
              delivered: 1,
              deliveredSubscriptions: ["sub-gap"],
              ok: true,
            })
          );
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
          subscriptionId: "sub-gap",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: 0, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      recoveredByFullReevaluation: boolean;
    };
    const latestSequence = await getRealtimeOutboxSequence("gap-retained");
    const historyGapQueries = prepare.mock.calls.filter(
      ([sql]) =>
        sql.includes("MIN(sequence) AS oldest_sequence") &&
        sql.includes("MAX(sequence) AS latest_sequence")
    );

    expect(body).toEqual(
      expect.objectContaining({
        evaluated: 1,
        failed: 0,
        recoveredByFullReevaluation: true,
      })
    );
    expect(deliveries).toHaveLength(1);
    expect(getFirstRealtimeDelivery(deliveries[0]).sequence).toBe(
      latestSequence
    );

    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      lastProcessedOutboxSequence: number | null;
    };

    expect(registrationsBody.lastProcessedOutboxSequence).toBe(latestSequence);
    expect(historyGapQueries).toHaveLength(1);
  });

  it("advances catch-up cursor to afterSequence when retained gap rows are gone", async () => {
    const runtimeId = createRealtimeRuntimeId();
    await createTodoViaRpc("owner-a", "gap-empty-recovered");
    let historyGapQueries = 0;
    const database = {
      batch: env.APP_DB.batch.bind(env.APP_DB),
      prepare(queryText: string): D1PreparedStatement {
        if (
          queryText.includes("MIN(sequence) AS oldest_sequence") &&
          queryText.includes("MAX(sequence) AS latest_sequence")
        ) {
          historyGapQueries += 1;
          return {
            bind: () =>
              ({
                first: async () => ({
                  latest_sequence: null,
                  oldest_sequence: 200,
                }),
              }) as D1PreparedStatement,
          } as D1PreparedStatement;
        }
        return env.APP_DB.prepare(queryText);
      },
      withSession: env.APP_DB.withSession?.bind(env.APP_DB),
    } as D1Database;
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: database,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(() =>
        Promise.resolve(
          Response.json({
            delivered: 1,
            deliveredSubscriptions: ["sub-gap-empty"],
            ok: true,
          })
        )
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
          subscriptionId: "sub-gap-empty",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: 123, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      recoveredByFullReevaluation: boolean;
    };
    const cursor = await env.APP_DB.prepare(
      "SELECT last_processed_outbox_sequence FROM _bf_realtime_shard_cursors WHERE shard_name = ?"
    )
      .bind(getRealtimeSubscriptionShardName())
      .first<{ last_processed_outbox_sequence: number }>();

    expect(body.recoveredByFullReevaluation).toBe(true);
    expect(cursor?.last_processed_outbox_sequence).toBe(123);
    expect(historyGapQueries).toBe(1);
  });

  it("removes expired realtime outbox rows during catch-up cleanup", async () => {
    const expiredAt = Date.now() - 8 * 24 * 60 * 60 * 1000;
    const recentAt = Date.now();
    await createRealtimeOutboxEvent("expired-outbox-event", expiredAt);
    await createRealtimeOutboxEvent("recent-outbox-event", recentAt);
    const latest = await env.APP_DB.prepare(
      "SELECT MAX(sequence) AS sequence FROM _bf_realtime_outbox"
    ).first<{ sequence: number }>();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: latest?.sequence, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const expired = await env.APP_DB.prepare(
      "SELECT event_id FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("expired-outbox-event")
      .first<{ event_id: string }>();
    const recent = await env.APP_DB.prepare(
      "SELECT event_id FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("recent-outbox-event")
      .first<{ event_id: string }>();

    expect(response.status).toBe(200);
    expect(expired).toBeNull();
    expect(recent?.event_id).toBe("recent-outbox-event");
  });

  it("schedules realtime outbox cleanup alarms without delaying pending alarms", async () => {
    const now = 1_000_000;
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    const state = new FakeRealtimeDurableObjectState();
    const subscriptionDo = new RealtimeSubscriptionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    try {
      await subscriptionDo.fetch(
        new Request("https://baseflare.internal/register", {
          body: JSON.stringify({
            args: { ownerToken: "owner-a" },
            connectionKey: "client:cleanup-alarm",
            connectionName: getRealtimeConnectionShardName(
              "client:cleanup-alarm"
            ),
            epoch: 1,
            queryName: "todos:list",
            runtimeId: "runtime:1",
            subscriptionId: "sub-cleanup-alarm",
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );
      const pendingAlarm = state.alarmTime;
      expect(pendingAlarm).toBe(now + REALTIME_OUTBOX_CLEANUP_INTERVAL_MS);

      nowSpy.mockReturnValue(now + 10_000);
      await subscriptionDo.fetch(
        new Request("https://baseflare.internal/adopt-registration", {
          body: JSON.stringify({
            args: { ownerToken: "owner-a" },
            connectionKey: "client:cleanup-alarm",
            connectionName: getRealtimeConnectionShardName(
              "client:cleanup-alarm"
            ),
            epoch: 2,
            queryName: "todos:list",
            runtimeId: "runtime:1",
            subscriptionId: "sub-cleanup-alarm",
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

      expect(state.alarmTime).toBe(pendingAlarm);
    } finally {
      nowSpy.mockRestore();
    }
  });

  it("runs realtime outbox cleanup from subscription alarms and reschedules", async () => {
    const now = Date.now();
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    const expiredAt = now - 8 * 24 * 60 * 60 * 1000;
    const recentAt = now;
    await createRealtimeOutboxEvent("alarm-expired-outbox-event", expiredAt);
    await createRealtimeOutboxEvent("alarm-recent-outbox-event", recentAt);
    const state = new FakeRealtimeDurableObjectState();
    state.alarmTime = now;
    const subscriptionDo = new RealtimeSubscriptionDO(state, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    try {
      await subscriptionDo.alarm();
    } finally {
      nowSpy.mockRestore();
    }

    const expired = await env.APP_DB.prepare(
      "SELECT event_id FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("alarm-expired-outbox-event")
      .first<{ event_id: string }>();
    const recent = await env.APP_DB.prepare(
      "SELECT event_id FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("alarm-recent-outbox-event")
      .first<{ event_id: string }>();

    expect(expired).toBeNull();
    expect(recent?.event_id).toBe("alarm-recent-outbox-event");
    expect(state.alarmTime).toBe(now + REALTIME_OUTBOX_CLEANUP_INTERVAL_MS);
  });

  it("reschedules realtime outbox cleanup alarms after cleanup failure", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const now = Date.now();
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    const state = new FakeRealtimeDurableObjectState();
    state.alarmTime = now;
    const failingDatabase = {
      batch: env.APP_DB.batch.bind(env.APP_DB),
      prepare(sql: string) {
        if (sql.includes("MIN(last_processed_outbox_sequence)")) {
          throw new Error("cleanup unavailable");
        }

        return env.APP_DB.prepare(sql);
      },
    } as unknown as D1Database;
    const subscriptionDo = new RealtimeSubscriptionDO(state, {
      APP_DB: failingDatabase,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    try {
      await subscriptionDo.alarm();
    } finally {
      nowSpy.mockRestore();
    }

    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        errorName: "DatabaseRuntimeError",
        event: "runtime.realtime_outbox_cleanup_failed",
      })
    );
    expect(state.alarmTime).toBe(now + REALTIME_OUTBOX_CLEANUP_INTERVAL_MS);
  });

  it("bounds and throttles realtime outbox cleanup work", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const expiredAt = Date.now() - 8 * 24 * 60 * 60 * 1000;
    await env.APP_DB.prepare(
      `WITH RECURSIVE events(index_value) AS (
         SELECT 1
         UNION ALL
         SELECT index_value + 1 FROM events WHERE index_value < 1002
       )
       INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions)
       SELECT 'expired-batch-' || index_value, ?, ?, ? FROM events`
    )
      .bind(expiredAt, JSON.stringify(["todos"]), JSON.stringify([]))
      .run();
    const latest = await env.APP_DB.prepare(
      "SELECT MAX(sequence) AS sequence FROM _bf_realtime_outbox"
    ).first<{ sequence: number }>();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const catchUp = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/catch-up", {
          body: JSON.stringify({ afterSequence: latest?.sequence, limit: 10 }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await catchUp();
    await catchUp();

    let remaining = await env.APP_DB.prepare(
      "SELECT COUNT(*) AS count FROM _bf_realtime_outbox WHERE created_at = ?"
    )
      .bind(expiredAt)
      .first<{ count: number }>();

    expect(remaining?.count).toBe(2);
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.outbox_cleanups",
        tags: { result: "limited" },
        value: 1000,
      })
    );

    await env.APP_DB.prepare("DELETE FROM _bf_realtime_shard_cursors").run();
    const secondSubscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await secondSubscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: latest?.sequence, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    remaining = await env.APP_DB.prepare(
      "SELECT COUNT(*) AS count FROM _bf_realtime_outbox WHERE created_at = ?"
    )
      .bind(expiredAt)
      .first<{ count: number }>();

    expect(remaining?.count).toBe(1);
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.outbox_cleanups",
        tags: { result: "cleaned" },
        value: 1,
      })
    );
  });

  it("throttles realtime outbox cleanup after a cleanup failure", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const expiredAt = Date.now() - 8 * 24 * 60 * 60 * 1000;
    const recentAt = Date.now();
    await createRealtimeOutboxEvent("retry-expired-outbox-event", expiredAt);
    await createRealtimeOutboxEvent("retry-recent-outbox-event", recentAt);
    const latest = await env.APP_DB.prepare(
      "SELECT MAX(sequence) AS sequence FROM _bf_realtime_outbox"
    ).first<{ sequence: number }>();
    let failCursorRead = true;
    let cursorReadAttempts = 0;
    const flakyDatabase = {
      batch: env.APP_DB.batch.bind(env.APP_DB),
      prepare(sql: string) {
        if (sql.includes("MIN(last_processed_outbox_sequence)")) {
          cursorReadAttempts += 1;
          if (failCursorRead) {
            failCursorRead = false;
            throw new Error("cursor read unavailable");
          }
        }

        return env.APP_DB.prepare(sql);
      },
    } as unknown as D1Database;
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: flakyDatabase,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const catchUp = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/catch-up", {
          body: JSON.stringify({ afterSequence: latest?.sequence, limit: 10 }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await catchUp();
    await catchUp();

    let remaining = await env.APP_DB.prepare(
      "SELECT COUNT(*) AS count FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("retry-expired-outbox-event")
      .first<{ count: number }>();

    expect(cursorReadAttempts).toBe(1);
    expect(remaining?.count).toBe(1);
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        errorName: "DatabaseRuntimeError",
        event: "runtime.realtime_outbox_cleanup_failed",
      })
    );

    const internals = subscriptionDo as unknown as {
      lastOutboxCleanupAt: number;
    };
    internals.lastOutboxCleanupAt = 0;
    await catchUp();

    remaining = await env.APP_DB.prepare(
      "SELECT COUNT(*) AS count FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("retry-expired-outbox-event")
      .first<{ count: number }>();

    expect(cursorReadAttempts).toBe(2);
    expect(remaining?.count).toBe(0);
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.outbox_cleanups",
        tags: { result: "cleaned" },
      })
    );
  });

  it("re-evaluates active realtime registrations during catch-up", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const delivery = await request.json();
        deliveries.push(delivery);
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
    const delivery = getFirstRealtimeDelivery(deliveries[0]);
    expect(delivery.sequence).toBe(body.events[0]?.sequence);
    expect(delivery.subscriptionId).toBe("sub-good");
    expect(delivery.result.map((todo) => todo.text)).toEqual([
      "catch-up-delivered",
    ]);
  });

  it("advances realtime catch-up cursor only after delivery evaluation", async () => {
    await createTodoViaRpc("owner-a", "catch-up-cursor-order");
    await createRealtimeOutboxEvent("catch-up-cursor-order");
    const row = await env.APP_DB.prepare(
      "SELECT sequence FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("catch-up-cursor-order")
      .first<{ sequence: number }>();
    const runtimeId = createRealtimeRuntimeId();
    let cursorDuringDelivery: number | null | undefined;
    let subscriptionDo: RealtimeSubscriptionDO;
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/deliver") {
          const registrationsResponse = await subscriptionDo.fetch(
            new Request("https://baseflare.internal/registrations", {
              body: "{}",
              method: "POST",
            })
          );
          const registrationsBody = (await registrationsResponse.json()) as {
            lastProcessedOutboxSequence: number | null;
          };
          cursorDuringDelivery = registrationsBody.lastProcessedOutboxSequence;
          const delivery = await request.json();
          const items = getRealtimeDeliveryItems(delivery);
          return Response.json({
            delivered: items.length,
            deliveredSubscriptions: items.map((item) => item.subscriptionId),
            ok: true,
          });
        }
        return Response.json({ ok: true });
      }
    );
    subscriptionDo = new RealtimeSubscriptionDO(null, {
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
    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      lastProcessedOutboxSequence: number | null;
    };

    expect(response.status).toBe(200);
    expect(cursorDuringDelivery).toBeNull();
    expect(registrationsBody.lastProcessedOutboxSequence).toBe(row?.sequence);
  });

  it("requires realtime shard context before recording cursors", async () => {
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const advanceCursor = (
      subscriptionDo as unknown as {
        advanceLastProcessedOutboxSequence(sequence: number): Promise<void>;
      }
    ).advanceLastProcessedOutboxSequence.bind(subscriptionDo);

    await expect(advanceCursor(1)).rejects.toThrow(
      "Realtime shard cursor cannot advance without shard context"
    );
  });

  it("skips the D1 re-query when subscription versions are unchanged", async () => {
    const runtimeId = createRealtimeRuntimeId();
    // Seed a todo so the table version is non-zero and captured in the snapshot.
    await createTodoViaRpc("owner-a", "seed");
    const deliveries: unknown[] = [];
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          const delivery = await request.json();
          deliveries.push(delivery);
          const items = getRealtimeDeliveryItems(delivery);
          return Response.json({
            delivered: items.length,
            deliveredSubscriptions: items.map((item) => item.subscriptionId),
            ok: true,
          });
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
          subscriptionId: "sub-unchanged",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    const catchUp = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/catch-up", {
          body: JSON.stringify({ afterSequence: null, limit: 10 }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    // First catch-up captures the dependency + version snapshot (evaluates once).
    const firstBody = (await (await catchUp()).json()) as { evaluated: number };
    expect(firstBody.evaluated).toBe(1);
    const deliveredAfterFirst = deliveries.length;

    // A later outbox event lands for the same dependency WITHOUT bumping the
    // version. Version-first reconciliation must skip the D1 re-query entirely.
    await createRealtimeOutboxEvent("no-version-change", Date.now(), {
      bumpVersions: false,
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });

    const response = await catchUp();
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
    };

    expect(response.status).toBe(200);
    // evaluated counts registrations whose query was re-run against D1; zero
    // proves the dependent registration was skipped on the matching version.
    expect(body).toEqual(expect.objectContaining({ evaluated: 0, failed: 0 }));
    expect(deliveries).toHaveLength(deliveredAfterFirst);
  });

  it("heals a missed delivery during live reconciliation while the socket stays open", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const sentFrames: Array<{
      message: { result: Array<{ text: string }>; subscriptionId: string };
      type: string;
    }> = [];
    const socket = {
      send(payload: string) {
        sentFrames.push(JSON.parse(payload));
      },
    } as unknown as WebSocket;

    let connectionDo: RealtimeConnectionDO;
    const subscriptions = new FakeDurableObjectNamespace(
      async (_name, _request) => {
        // Stand in for the subscription DO: a catch-up heals the missed update by
        // delivering it back to the still-open connection socket, as in production.
        await connectionDo.fetch(
          new Request("https://baseflare.internal/deliver", {
            body: JSON.stringify({
              connectionKey: "client-a",
              deliveries: [
                {
                  result: [{ text: "healed" }],
                  sequence: 7,
                  subscriptionId: "sub-a",
                },
              ],
            }),
            headers: { "content-type": "application/json" },
            method: "POST",
          })
        );
        return Response.json({ evaluated: 1, events: [], failed: 0, ok: true });
      }
    );
    connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: subscriptions,
    });

    const internals = connectionDo as unknown as {
      sockets: Set<WebSocket>;
      socketStates: Map<WebSocket, { attachment: Record<string, unknown> }>;
      socketsByConnectionKey: Map<string, Set<WebSocket>>;
    };
    internals.sockets.add(socket);
    internals.socketStates.set(socket, {
      attachment: {
        authorizationHeader: "Bearer owner-a",
        connectionKey: "client-a",
        connectionName: getRealtimeConnectionShardName("client-a"),
        latestDeliveredOutboxSequence: 1,
        runtimeId,
        subscriptions: [
          {
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-a",
            subscriptionShardName: getRealtimeSubscriptionShardName(),
          },
        ],
      },
    });
    internals.socketsByConnectionKey.set("client-a", new Set([socket]));

    await connectionDo.alarm();

    expect(sentFrames).toEqual([
      {
        message: {
          result: [{ text: "healed" }],
          sequence: 7,
          subscriptionId: "sub-a",
        },
        type: "delivery",
      },
    ]);
  });

  it("uses the best available delivered sequence for live reconciliation", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const catchUpBodies: Array<{ afterSequence: number | null }> = [];
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          catchUpBodies.push(
            (await request.json()) as { afterSequence: number | null }
          );
          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }
      ),
    });
    const anchoredSocket = { send: () => undefined } as unknown as WebSocket;
    const freshSocket = { send: () => undefined } as unknown as WebSocket;
    const internals = connectionDo as unknown as {
      socketStates: Map<WebSocket, { attachment: Record<string, unknown> }>;
    };
    const subscription = {
      args: { ownerToken: "owner-a" },
      epoch: 1,
      queryName: "todos:list",
      subscriptionId: "sub-a",
      subscriptionShardName: getRealtimeSubscriptionShardName(),
    };
    internals.socketStates.set(anchoredSocket, {
      attachment: {
        authorizationHeader: "Bearer owner-a",
        connectionKey: "client-a",
        connectionName: getRealtimeConnectionShardName("client-a"),
        latestDeliveredOutboxSequence: 42,
        runtimeId,
        subscriptions: [subscription],
      },
    });
    internals.socketStates.set(freshSocket, {
      attachment: {
        authorizationHeader: "Bearer owner-b",
        connectionKey: "client-b",
        connectionName: getRealtimeConnectionShardName("client-b"),
        latestDeliveredOutboxSequence: null,
        runtimeId,
        subscriptions: [
          {
            ...subscription,
            subscriptionId: "sub-b",
          },
        ],
      },
    });

    await connectionDo.alarm();

    expect(catchUpBodies).toEqual([
      expect.objectContaining({ afterSequence: 42 }),
    ]);
  });

  it("uses shard-scoped delivered sequences for live reconciliation", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const catchUps: Array<{
      afterSequence: number | null;
      shardName: string;
    }> = [];
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          catchUps.push(
            (await request.json()) as {
              afterSequence: number | null;
              shardName: string;
            }
          );
          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }
      ),
    });
    const shardA = "subscription:g2:0";
    const shardB = "subscription:g2:1";
    const shardC = "subscription:g2:2";
    const socketA = { send: () => undefined } as unknown as WebSocket;
    const socketB = { send: () => undefined } as unknown as WebSocket;
    const socketC = { send: () => undefined } as unknown as WebSocket;
    const internals = connectionDo as unknown as {
      socketStates: Map<WebSocket, { attachment: Record<string, unknown> }>;
    };
    const createSubscription = (
      subscriptionId: string,
      subscriptionShardName: string
    ): Record<string, unknown> => ({
      args: { ownerToken: subscriptionId },
      epoch: 1,
      queryName: "todos:list",
      subscriptionId,
      subscriptionShardName,
    });
    internals.socketStates.set(socketA, {
      attachment: {
        authorizationHeader: "Bearer owner-a",
        connectionKey: "client-a",
        connectionName: getRealtimeConnectionShardName("client-a"),
        latestDeliveredOutboxSequence: 7,
        runtimeId,
        subscriptions: [createSubscription("sub-a", shardA)],
      },
    });
    internals.socketStates.set(socketB, {
      attachment: {
        authorizationHeader: "Bearer owner-b",
        connectionKey: "client-b",
        connectionName: getRealtimeConnectionShardName("client-b"),
        latestDeliveredOutboxSequence: 42,
        runtimeId,
        subscriptions: [createSubscription("sub-b", shardB)],
      },
    });
    internals.socketStates.set(socketC, {
      attachment: {
        authorizationHeader: "Bearer owner-c",
        connectionKey: "client-c",
        connectionName: getRealtimeConnectionShardName("client-c"),
        latestDeliveredOutboxSequence: null,
        runtimeId,
        subscriptions: [createSubscription("sub-c", shardC)],
      },
    });

    await connectionDo.alarm();

    expect(catchUps).toEqual(
      expect.arrayContaining([
        { afterSequence: 7, shardName: shardA },
        { afterSequence: 42, shardName: shardB },
        { afterSequence: null, shardName: shardC },
      ])
    );
  });

  it("uses conservative live reconciliation when no socket has a delivered sequence", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const catchUpBodies: Array<{ afterSequence: number | null }> = [];
    const connectionDo = new RealtimeConnectionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          catchUpBodies.push(
            (await request.json()) as { afterSequence: number | null }
          );
          return Response.json({
            evaluated: 0,
            events: [],
            failed: 0,
            ok: true,
          });
        }
      ),
    });
    const socket = { send: () => undefined } as unknown as WebSocket;
    const internals = connectionDo as unknown as {
      socketStates: Map<WebSocket, { attachment: Record<string, unknown> }>;
    };
    internals.socketStates.set(socket, {
      attachment: {
        authorizationHeader: "Bearer owner-a",
        connectionKey: "client-a",
        connectionName: getRealtimeConnectionShardName("client-a"),
        latestDeliveredOutboxSequence: null,
        runtimeId,
        subscriptions: [
          {
            args: { ownerToken: "owner-a" },
            epoch: 1,
            queryName: "todos:list",
            subscriptionId: "sub-a",
            subscriptionShardName: getRealtimeSubscriptionShardName(),
          },
        ],
      },
    });

    await connectionDo.alarm();

    expect(catchUpBodies).toEqual([
      expect.objectContaining({ afterSequence: null }),
    ]);
  });

  it("skips realtime notify re-evaluation for unrelated partitions", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const delivery = await request.json();
        deliveries.push(delivery);
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const notify = (eventId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await createTodoViaRpc("owner-a", "owner-a-first");
    await createRealtimeOutboxEvent("owner-a-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await notify("owner-a-prime");
    await createTodoViaRpc("owner-b", "owner-b-only");
    await createRealtimeOutboxEvent("owner-b-unrelated", Date.now(), {
      partitions: [todoOwnerPartition("owner-b")],
      tables: ["todos"],
    });
    const unrelatedResponse = await notify("owner-b-unrelated");

    await createTodoViaRpc("owner-a", "owner-a-second");
    await createRealtimeOutboxEvent("owner-a-relevant", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    const relevantResponse = await notify("owner-a-relevant");

    expect(await unrelatedResponse.json()).toEqual({
      evaluated: 0,
      failed: 0,
      ok: true,
    });
    expect(await relevantResponse.json()).toEqual({
      evaluated: 1,
      failed: 0,
      ok: true,
    });
    expect(realtimeDependencyTrackingQueryCalls).toBe(2);
    expect(deliveries).toHaveLength(2);
  });

  it("batches relevant realtime version-gap reads", async () => {
    const runtimeId = createRealtimeRuntimeId();
    let tableVersionReads = 0;
    let partitionVersionReads = 0;
    const tableVersionBindSizes: number[] = [];
    const partitionVersionBindSizes: number[] = [];
    const database: D1Database = {
      batch: (statements) => env.APP_DB.batch(statements),
      prepare(sql) {
        const statement = env.APP_DB.prepare(sql);
        if (sql.includes("FROM _bf_table_versions")) {
          tableVersionReads += 1;
        }
        if (sql.includes("_bf_partition_versions")) {
          partitionVersionReads += 1;
        }
        return new Proxy(statement, {
          get(target, property, receiver) {
            if (property !== "bind") {
              return Reflect.get(target, property, receiver);
            }

            return (...values: Parameters<D1PreparedStatement["bind"]>) => {
              if (sql.includes("FROM _bf_table_versions")) {
                tableVersionBindSizes.push(values.length);
              }
              if (sql.includes("_bf_partition_versions")) {
                partitionVersionBindSizes.push(values.length);
              }
              return target.bind(...values);
            };
          },
        });
      },
    };
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: database,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
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
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const ownerAPartitionId = todoOwnerPartitionId("owner-a");
    const ownerBPartitionId = todoOwnerPartitionId("owner-b");
    const ownerCPartitionId = todoOwnerPartitionId("owner-c");
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const registration = indexState.registrations.get(registrationKey) as
      | {
          dependencies?: {
            partitions: Set<string>;
            tables: Set<string>;
          };
          versionSnapshot?: {
            partitions: ReadonlyMap<string, number>;
            tables: ReadonlyMap<string, number>;
          };
        }
      | undefined;
    if (!registration) {
      throw new Error("Missing realtime registration");
    }
    const dependencies = {
      partitions: new Set([
        ownerAPartitionId,
        ownerBPartitionId,
        ownerCPartitionId,
      ]),
      tables: new Set(["comments", "labels", "todos"]),
    };
    registration.dependencies = dependencies;
    registration.versionSnapshot = await fetchRealtimeVersionSnapshot(
      env.APP_DB,
      dependencies
    );
    tableVersionReads = 0;
    partitionVersionReads = 0;
    indexState.registrationKeysByTable.set(
      "labels",
      new Set([registrationKey])
    );
    indexState.registrationKeysByTable.set("todos", new Set([registrationKey]));
    indexState.registrationKeysByPartition.set(
      ownerAPartitionId,
      new Set([registrationKey])
    );
    indexState.registrationKeysByPartition.set(
      ownerBPartitionId,
      new Set([registrationKey])
    );
    indexState.registrationKeysWithoutDependencies.delete(registrationKey);
    await createRealtimeOutboxEvent("batched-version-gap", Date.now(), {
      bumpVersions: false,
      partitions: [
        todoOwnerPartition("owner-a"),
        todoOwnerPartition("owner-b"),
      ],
      tables: ["labels", "todos"],
    });

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "batched-version-gap" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(await response.json()).toEqual({
      evaluated: 0,
      failed: 0,
      ok: true,
    });
    expect(tableVersionReads).toBe(1);
    expect(partitionVersionReads).toBe(1);
    expect(tableVersionBindSizes).toEqual([2]);
    expect(partitionVersionBindSizes).toEqual([8]);
  });

  it("indexes realtime registrations after dependency capture", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace((_name, request) =>
        acknowledgeRealtimeDeliveryRequest(request)
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const indexState = getRealtimeIndexTestState(subscriptionDo);

    expect(
      indexState.registrationKeysWithoutDependencies.has(registrationKey)
    ).toBe(true);

    await createTodoViaRpc("owner-a", "owner-a-indexed");
    await createRealtimeOutboxEvent("owner-a-index-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "owner-a-index-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(
      indexState.registrationKeysWithoutDependencies.has(registrationKey)
    ).toBe(false);
    expect(
      indexState.registrationKeysByPartition
        .get(todoOwnerPartitionId("owner-a"))
        ?.has(registrationKey)
    ).toBe(true);
  });

  it("indexes realtime registrations from provided dependency metadata", async () => {
    const runtimeId = createRealtimeRuntimeId();
    let deliveryCalls = 0;
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          deliveryCalls += 1;
          return await acknowledgeRealtimeDeliveryRequest(request);
        }
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
    const ownerPartitionId = todoOwnerPartitionId("owner-a");
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          dependencies: {
            partitions: [ownerPartitionId],
            tables: [],
          },
          epoch: 1,
          lastResultJson: JSON.stringify([]),
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-a",
          versionSnapshot: {
            partitions: [[ownerPartitionId, 0]],
            tables: [],
          },
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const indexState = getRealtimeIndexTestState(subscriptionDo);

    expect(
      indexState.registrationKeysWithoutDependencies.has(registrationKey)
    ).toBe(false);
    expect(
      indexState.registrationKeysByPartition
        .get(ownerPartitionId)
        ?.has(registrationKey)
    ).toBe(true);

    await createRealtimeOutboxEvent("preindexed-unrelated", Date.now(), {
      partitions: [],
      tables: ["labels"],
    });
    const notifyResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "preindexed-unrelated" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(await notifyResponse.json()).toEqual({
      evaluated: 0,
      failed: 0,
      ok: true,
    });
    expect(deliveryCalls).toBe(0);
  });

  it("keeps empty realtime dependency snapshots conservatively indexed", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          const delivery = await request.json();
          deliveries.push(delivery);
          const items = getRealtimeDeliveryItems(delivery);
          return Response.json({
            delivered: items.length,
            deliveredSubscriptions: items.map((item) => item.subscriptionId),
            ok: true,
          });
        }
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { label: "stable" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:noRead",
          runtimeId,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createRealtimeOutboxEvent("no-read-prime", Date.now(), {
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "no-read-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    const indexState = getRealtimeIndexTestState(subscriptionDo);
    expect(
      indexState.registrationKeysWithoutDependencies.has(registrationKey)
    ).toBe(true);
    expect(indexState.registrationKeysByTable.size).toBe(0);
    expect(indexState.registrationKeysByPartition.size).toBe(0);

    await createRealtimeOutboxEvent("no-read-follow-up", Date.now(), {
      tables: ["labels"],
    });
    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "no-read-follow-up" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(await response.json()).toEqual({
      evaluated: 1,
      failed: 0,
      ok: true,
    });
    expect(deliveries).toHaveLength(1);
  });

  it("routes broad table events to partition-indexed realtime registrations", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace((_name, request) =>
        acknowledgeRealtimeDeliveryRequest(request)
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "owner-a-broad-prime");
    await createRealtimeOutboxEvent("owner-a-broad-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "owner-a-broad-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    realtimeDependencyTrackingQueryCalls = 0;
    await createRealtimeOutboxEvent("todos-broad-table", Date.now(), {
      tables: ["todos"],
    });

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "todos-broad-table" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(await response.json()).toEqual({
      evaluated: 1,
      failed: 0,
      ok: true,
    });
    expect(realtimeDependencyTrackingQueryCalls).toBe(1);
  });

  it("updates realtime dependency indexes when snapshots move", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace((_name, request) =>
        acknowledgeRealtimeDeliveryRequest(request)
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: {
            mode: "dynamic-partition-todos",
            ownerToken: "owner-a",
          },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "owner-a-dynamic");
    await createRealtimeOutboxEvent("dynamic-owner-a-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "dynamic-owner-a-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    realtimeDynamicDependencyOwnerToken = "owner-b";
    await createTodoViaRpc("owner-b", "owner-b-dynamic");
    await createRealtimeOutboxEvent("dynamic-owner-a-move", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "dynamic-owner-a-move" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const indexState = getRealtimeIndexTestState(subscriptionDo);

    expect(
      indexState.registrationKeysByPartition
        .get(todoOwnerPartitionId("owner-a"))
        ?.has(registrationKey)
    ).not.toBe(true);
    expect(
      indexState.registrationKeysByPartition
        .get(todoOwnerPartitionId("owner-b"))
        ?.has(registrationKey)
    ).toBe(true);
  });

  it("removes unregistered realtime registrations from dependency indexes", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace((_name, request) =>
        acknowledgeRealtimeDeliveryRequest(request)
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "owner-a-unregister-index");
    await createRealtimeOutboxEvent("owner-a-unregister-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "owner-a-unregister-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/unregister", {
        body: JSON.stringify({
          connectionKey: "client-a",
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const indexState = getRealtimeIndexTestState(subscriptionDo);

    expect(
      indexState.registrationKeysWithoutDependencies.has(registrationKey)
    ).toBe(false);
    expect(
      indexState.registrationKeysByPartition
        .get(todoOwnerPartitionId("owner-a"))
        ?.has(registrationKey)
    ).not.toBe(true);
  });

  it("keeps realtime dependency indexes after failed re-evaluation", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace((_name, request) =>
        acknowledgeRealtimeDeliveryRequest(request)
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "owner-a-failed-index");
    await createRealtimeOutboxEvent("owner-a-failed-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "owner-a-failed-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    resetRealtimeRuntimeStateForTest();
    await createRealtimeOutboxEvent("owner-a-failed-runtime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "owner-a-failed-runtime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const indexState = getRealtimeIndexTestState(subscriptionDo);

    expect(await response.json()).toEqual({
      evaluated: 0,
      failed: 1,
      ok: true,
    });
    expect(
      indexState.registrationKeysByPartition
        .get(todoOwnerPartitionId("owner-a"))
        ?.has(registrationKey)
    ).toBe(true);
  });

  it("removes expired realtime registrations from dependency indexes", async () => {
    const runtimeId = createRealtimeRuntimeId();
    let shouldDeliver = true;
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(() =>
        Promise.resolve(
          shouldDeliver
            ? Response.json({
                delivered: 1,
                deliveredSubscriptions: ["sub-a"],
                ok: true,
              })
            : Response.json({
                delivered: 0,
                deliveredSubscriptions: [],
                ok: true,
              })
        )
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "owner-a-expired-index");
    await createRealtimeOutboxEvent("owner-a-expired-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "owner-a-expired-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const registration = indexState.registrations.get(registrationKey);
    expect(registration).toBeDefined();
    if (registration) {
      registration.leaseExpiresAt = Date.now() - 1;
    }
    shouldDeliver = false;
    await createTodoViaRpc("owner-a", "owner-a-expired-index-next");
    await createRealtimeOutboxEvent("owner-a-expired-delete", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });

    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "owner-a-expired-delete" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(indexState.registrations.has(registrationKey)).toBe(false);
    expect(
      indexState.registrationKeysByPartition
        .get(todoOwnerPartitionId("owner-a"))
        ?.has(registrationKey)
    ).not.toBe(true);
  });

  it("keeps broad table dependencies subscribed to table events", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace((_name, request) =>
        acknowledgeRealtimeDeliveryRequest(request)
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "broad-labels", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-labels",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const notify = (eventId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await createRealtimeOutboxEvent("labels-prime", Date.now(), {
      tables: ["labels"],
    });
    await notify("labels-prime");
    await createRealtimeOutboxEvent("todos-unrelated-to-labels", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    const unrelatedResponse = await notify("todos-unrelated-to-labels");
    await createRealtimeOutboxEvent("labels-relevant", Date.now(), {
      tables: ["labels"],
    });
    const relevantResponse = await notify("labels-relevant");

    expect(await unrelatedResponse.json()).toEqual({
      evaluated: 0,
      failed: 0,
      ok: true,
    });
    expect(await relevantResponse.json()).toEqual({
      evaluated: 1,
      failed: 0,
      ok: true,
    });
    expect(realtimeDependencyTrackingQueryCalls).toBe(2);
  });

  it("treats point reads as table dependencies", async () => {
    const id = await createTodoViaRpc("owner-a", "point-read");
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace((_name, request) =>
        acknowledgeRealtimeDeliveryRequest(request)
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { id, mode: "get-todo", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-get",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const notify = (eventId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await createRealtimeOutboxEvent("point-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await notify("point-prime");
    await createRealtimeOutboxEvent("point-table-match", Date.now(), {
      partitions: [todoOwnerPartition("owner-b")],
      tables: ["todos"],
    });
    const response = await notify("point-table-match");

    expect(await response.json()).toEqual({
      evaluated: 1,
      failed: 0,
      ok: true,
    });
    expect(realtimeDependencyTrackingQueryCalls).toBe(2);
  });

  it("filters realtime catch-up by combined event dependencies", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace((_name, request) =>
        acknowledgeRealtimeDeliveryRequest(request)
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-catch-up",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createRealtimeOutboxEvent("catch-up-prime", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "catch-up-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const afterSequence = await getRealtimeOutboxSequence("catch-up-prime");
    await createRealtimeOutboxEvent("catch-up-owner-b", Date.now(), {
      partitions: [todoOwnerPartition("owner-b")],
      tables: ["todos"],
    });
    await createRealtimeOutboxEvent("catch-up-labels", Date.now(), {
      tables: ["labels"],
    });

    const unrelatedResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createRealtimeOutboxEvent("catch-up-owner-a", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });
    const relevantResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence, limit: 10 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(await unrelatedResponse.json()).toEqual(
      expect.objectContaining({
        evaluated: 0,
        failed: 0,
        ok: true,
      })
    );
    expect(await relevantResponse.json()).toEqual(
      expect.objectContaining({
        evaluated: 1,
        failed: 0,
        ok: true,
      })
    );
    expect(realtimeDependencyTrackingQueryCalls).toBe(2);
  });

  it("emits realtime catch-up re-evaluation and outbox lag metrics", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace((_name, request) =>
        acknowledgeRealtimeDeliveryRequest(request)
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "partition-todos", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-catch-up-metric",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createRealtimeOutboxEvent(
      "catch-up-metric-prime",
      Date.now() - 1000,
      {
        partitions: [todoOwnerPartition("owner-a")],
        tables: ["todos"],
      }
    );
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "catch-up-metric-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const afterSequence = await getRealtimeOutboxSequence(
      "catch-up-metric-prime"
    );
    await createRealtimeOutboxEvent(
      "catch-up-metric-unrelated",
      Date.now() - 1000,
      {
        partitions: [todoOwnerPartition("owner-b")],
        tables: ["todos"],
      }
    );
    info.mockClear();

    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.re_evaluations",
        tags: { result: "skipped", source: "catch_up" },
        value: 1,
      })
    );
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.outbox_lag_ms",
        tags: { source: "catch_up" },
      })
    );
  });

  it("uses realtime outbox sequence instead of event id for catch-up", async () => {
    const now = Date.now();
    await env.APP_DB.batch([
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind("z-event", now, JSON.stringify(["todos"]), JSON.stringify([])),
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind(
        "a-event",
        now + 1,
        JSON.stringify(["labels"]),
        JSON.stringify([])
      ),
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

  it("bounds realtime catch-up event reads", async () => {
    await env.APP_DB.prepare(`
      WITH RECURSIVE events(index_value) AS (
        SELECT 0
        UNION ALL
        SELECT index_value + 1 FROM events WHERE index_value < ${REALTIME_CATCH_UP_EVENT_LIMIT}
      )
      INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions)
      SELECT 'catch-up-limit-' || index_value, ?, ?, ? FROM events
    `)
      .bind(Date.now() - 1000, JSON.stringify(["todos"]), JSON.stringify([]))
      .run();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/catch-up", {
        body: JSON.stringify({ afterSequence: null, limit: 10_000 }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      events: Array<{ eventId: string; sequence: number }>;
      ok: boolean;
    };

    expect(body.ok).toBe(true);
    expect(body.events).toHaveLength(REALTIME_CATCH_UP_EVENT_LIMIT);
    expect(body.events.at(0)?.eventId).toBe("catch-up-limit-0");
    expect(body.events.at(-1)?.eventId).toBe(
      `catch-up-limit-${REALTIME_CATCH_UP_EVENT_LIMIT - 1}`
    );
  });

  it("advances realtime notify cursors by outbox sequence", async () => {
    await env.APP_DB.prepare(
      "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
    )
      .bind(
        "notify-event",
        Date.now(),
        JSON.stringify(["todos"]),
        JSON.stringify([])
      )
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
      lastProcessedOutboxSequence: number | null;
    };
    const row = await env.APP_DB.prepare(
      "SELECT sequence FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("notify-event")
      .first<{ sequence: number }>();

    expect(response.status).toBe(200);
    expect(body.lastProcessedOutboxSequence).toBe(row?.sequence);
  });

  it("advances realtime notify cursor only after delivery evaluation", async () => {
    await createTodoViaRpc("owner-a", "cursor-order");
    await createRealtimeOutboxEvent("notify-cursor-order");
    const row = await env.APP_DB.prepare(
      "SELECT sequence FROM _bf_realtime_outbox WHERE event_id = ?"
    )
      .bind("notify-cursor-order")
      .first<{ sequence: number }>();
    const runtimeId = createRealtimeRuntimeId();
    let cursorDuringDelivery: number | null | undefined;
    let subscriptionDo: RealtimeSubscriptionDO;
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/deliver") {
          const registrationsResponse = await subscriptionDo.fetch(
            new Request("https://baseflare.internal/registrations", {
              body: "{}",
              method: "POST",
            })
          );
          const registrationsBody = (await registrationsResponse.json()) as {
            lastProcessedOutboxSequence: number | null;
          };
          cursorDuringDelivery = registrationsBody.lastProcessedOutboxSequence;
          const delivery = await request.json();
          const items = getRealtimeDeliveryItems(delivery);
          return Response.json({
            delivered: items.length,
            deliveredSubscriptions: items.map((item) => item.subscriptionId),
            ok: true,
          });
        }
        return Response.json({ ok: true });
      }
    );
    subscriptionDo = new RealtimeSubscriptionDO(null, {
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
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "notify-cursor-order" }),
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
    const registrationsBody = (await registrationsResponse.json()) as {
      lastProcessedOutboxSequence: number | null;
    };

    expect(response.status).toBe(200);
    expect(cursorDuringDelivery).toBeNull();
    expect(registrationsBody.lastProcessedOutboxSequence).toBe(row?.sequence);
  });

  it("passes mutation commit bookmarks to realtime notify shards", async () => {
    const notifyBodies: unknown[] = [];
    const bookmark = "commit-bookmark";
    const session: D1DatabaseSession = {
      batch: (statements) => env.APP_DB.batch(statements),
      getBookmark: () => bookmark,
      prepare: (sql) => env.APP_DB.prepare(sql),
    };
    const database: D1Database = {
      batch: (statements) => env.APP_DB.batch(statements),
      prepare: (sql) => env.APP_DB.prepare(sql),
      withSession: () => session,
    };
    const runtimeEnv: BaseflareRuntimeEnv = {
      ...env,
      APP_DB: database,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          notifyBodies.push(await request.json());
          return Response.json({ ok: true });
        }
      ),
    };

    const response = await invoke(
      "/api/mutation/todos:create",
      {
        body: rpcBody({ ownerToken: "owner-a", text: "bookmark-notify" }),
        headers: { authorization: "Bearer owner-a" },
        method: "POST",
      },
      worker,
      runtimeEnv
    );

    expect(response.status).toBe(200);
    expect(notifyBodies).toEqual([
      expect.objectContaining({ outboxBookmark: bookmark }),
    ]);
  });

  it("uses realtime notify bookmarks for outbox event lookup", async () => {
    const sessionConstraints: string[] = [];
    const session: D1DatabaseSession = {
      batch: (statements) => env.APP_DB.batch(statements),
      getBookmark: () => "session-bookmark",
      prepare: (sql) => env.APP_DB.prepare(sql),
    };
    const database: D1Database = {
      batch: (statements) => env.APP_DB.batch(statements),
      prepare: (sql) => env.APP_DB.prepare(sql),
      withSession: (constraint) => {
        sessionConstraints.push(constraint ?? "");
        return session;
      },
    };
    await createRealtimeOutboxEvent("bookmark-lookup");
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: database,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "bookmark-lookup",
          outboxBookmark: "commit-bookmark",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(response.status).toBe(200);
    expect(sessionConstraints).toEqual(["commit-bookmark"]);
  });

  it("uses realtime notify bookmarks for version checks and query execution", async () => {
    realtimeDependencyTrackingQueryCalls = 0;
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const sessionPreparedSql: string[] = [];
    let useStaleBaseTableVersion = false;
    const staleTableVersionStatement = {
      all: async () => ({ results: [{ table_name: "labels", version: 0 }] }),
      bind() {
        return this;
      },
      first: async () => ({ version: 0 }),
      run: async () => ({ meta: {}, success: true }),
    } as unknown as ReturnType<D1Database["prepare"]>;
    const session: D1DatabaseSession = {
      batch: (statements) => env.APP_DB.batch(statements),
      getBookmark: () => "session-bookmark",
      prepare: (sql) => {
        sessionPreparedSql.push(sql);
        return env.APP_DB.prepare(sql);
      },
    };
    const database: D1Database = {
      batch: (statements) => env.APP_DB.batch(statements),
      prepare: (sql) => {
        if (
          useStaleBaseTableVersion &&
          sql.includes("FROM _bf_table_versions")
        ) {
          return staleTableVersionStatement;
        }

        return env.APP_DB.prepare(sql);
      },
      withSession: () => session,
    };
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: database,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          const delivery = await request.json();
          deliveries.push(delivery);
          const items = getRealtimeDeliveryItems(delivery);
          return Response.json({
            delivered: items.length,
            deliveredSubscriptions: items.map((item) => item.subscriptionId),
            ok: true,
          });
        }
      ),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/register", {
        body: JSON.stringify({
          args: { mode: "broad-labels", ownerToken: "owner-a" },
          authorizationHeader: "Bearer owner-a",
          connectionKey: "client-a",
          connectionName: "connection:0",
          epoch: 1,
          leaseExpiresAt: Date.now() + 60_000,
          queryName: "realtime:dependencyTracking",
          runtimeId,
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createRealtimeOutboxEvent("bookmark-version-prime", Date.now(), {
      tables: ["labels"],
    });
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "bookmark-version-prime" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    useStaleBaseTableVersion = true;
    await createRealtimeOutboxEvent("bookmark-version-check", Date.now(), {
      tables: ["labels"],
    });
    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "bookmark-version-check",
          outboxBookmark: "commit-bookmark",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      evaluated: 1,
      failed: 0,
      ok: true,
    });
    expect(realtimeDependencyTrackingQueryCalls).toBe(2);
    expect(deliveries).toHaveLength(1);
    expect(
      sessionPreparedSql.some((sql) => sql.includes("FROM _bf_table_versions"))
    ).toBe(true);
    expect(sessionPreparedSql.some((sql) => sql.includes("FROM labels"))).toBe(
      true
    );
  });

  it("logs and fails realtime notify when the outbox row is missing", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
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
      lastProcessedOutboxSequence: number | null;
    };

    expect(response.status).toBe(503);
    expect(body).toEqual({ evaluated: 0, failed: 0, ok: false });
    expect(deliveries).toHaveLength(0);
    expect(registrationsBody.lastProcessedOutboxSequence).toBeNull();
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.realtime_notify_outbox_event_missing",
        eventId: "missing-event",
      })
    );
  });

  it("fully recovers and advances realtime cursors after malformed notify rows", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    await createTodoViaRpc("owner-a", "malformed-notify-recovered");
    await env.APP_DB.prepare(
      "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
    )
      .bind("malformed-notify", Date.now(), "not-json", JSON.stringify([]))
      .run();
    const malformedSequence =
      await getRealtimeOutboxSequence("malformed-notify");
    const deliveries: unknown[] = [];
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          deliveries.push(await request.json());
          return Response.json({
            delivered: 1,
            deliveredSubscriptions: ["sub-malformed-notify"],
            ok: true,
          });
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
          subscriptionId: "sub-malformed-notify",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "malformed-notify" }),
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
      lastProcessedOutboxSequence: number | null;
    };

    expect(body).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(getFirstRealtimeDelivery(deliveries[0]).sequence).toBe(
      malformedSequence
    );
    expect(registrationsBody.lastProcessedOutboxSequence).toBe(
      malformedSequence
    );
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        errorName: "SyntaxError",
        event: "runtime.realtime_outbox_event_parse_failed",
        eventId: "malformed-notify",
      })
    );
  });

  it("fully recovers and advances realtime cursors after malformed catch-up rows", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    await createTodoViaRpc("owner-a", "malformed-catch-up-recovered");
    await env.APP_DB.prepare(
      "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
    )
      .bind("malformed-catch-up", Date.now(), JSON.stringify(["todos"]), "{")
      .run();
    const malformedSequence =
      await getRealtimeOutboxSequence("malformed-catch-up");
    const deliveries: unknown[] = [];
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(
        async (_name, request) => {
          deliveries.push(await request.json());
          return Response.json({
            delivered: 1,
            deliveredSubscriptions: ["sub-malformed-catch-up"],
            ok: true,
          });
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
          subscriptionId: "sub-malformed-catch-up",
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
      failed: number;
      recoveredByFullReevaluation: boolean;
    };
    const registrationsResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/registrations", {
        body: "{}",
        method: "POST",
      })
    );
    const registrationsBody = (await registrationsResponse.json()) as {
      lastProcessedOutboxSequence: number | null;
    };

    expect(body).toEqual(
      expect.objectContaining({
        evaluated: 1,
        failed: 0,
        recoveredByFullReevaluation: true,
      })
    );
    expect(getFirstRealtimeDelivery(deliveries[0]).sequence).toBe(
      malformedSequence
    );
    expect(registrationsBody.lastProcessedOutboxSequence).toBe(
      malformedSequence
    );
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        errorName: "SyntaxError",
        event: "runtime.realtime_outbox_event_parse_failed",
        eventId: "malformed-catch-up",
      })
    );
  });

  it("keeps realtime notify cursors monotonic for out-of-order events", async () => {
    const now = Date.now();
    await env.APP_DB.batch([
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind("older-event", now, JSON.stringify(["todos"]), JSON.stringify([])),
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind(
        "newer-event",
        now + 1,
        JSON.stringify(["todos"]),
        JSON.stringify([])
      ),
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
      lastProcessedOutboxSequence: number | null;
    };

    expect(body.lastProcessedOutboxSequence).toBe(newerRow?.sequence);
  });

  it("does not regress realtime cursors when catch-up returns older events", async () => {
    const now = Date.now();
    await env.APP_DB.batch([
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind(
        "catchup-older",
        now,
        JSON.stringify(["todos"]),
        JSON.stringify([])
      ),
      env.APP_DB.prepare(
        "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
      ).bind(
        "catchup-newer",
        now + 1,
        JSON.stringify(["todos"]),
        JSON.stringify([])
      ),
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
      lastProcessedOutboxSequence: number | null;
    };

    expect(body.lastProcessedOutboxSequence).toBe(newerRow?.sequence);
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
        const delivery = await request.json();
        deliveries.push(delivery);
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
    const delivery = getFirstRealtimeDelivery(deliveries[0]);
    expect(delivery.subscriptionId).toBe("sub-1");
    expect(delivery.connectionKey).toBe("client-a");
    expect(delivery.result.map((todo) => todo.text)).toEqual(["delivered"]);
  });

  it("emits realtime notify re-evaluation and delivery metrics", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/subscription-moved") {
          return Response.json({ ok: true });
        }

        const delivery = await request.json();
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
    await createTodoViaRpc("owner-a", "metric-delivered");
    await createRealtimeOutboxEvent("metric-notify-event", Date.now() - 1000);

    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "metric-notify-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.re_evaluations",
        tags: { result: "evaluated", source: "notify" },
        value: 1,
      })
    );
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.delivery_batches",
        tags: { result: "delivered" },
        value: 1,
      })
    );
    expect(info).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.outbox_lag_ms",
        tags: { source: "notify" },
      })
    );
  });

  it("keeps delivered realtime registrations current when dependency updates fail", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    await env.APP_DB.prepare(
      "UPDATE _bf_realtime_shard_generations SET subscription_shard_count = 8 WHERE generation_id = 1"
    ).run();
    const globalShardName = getRealtimeSubscriptionShardName(
      createRealtimeGlobalSubscriptionRouteTarget(),
      8
    );
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const delivery = await request.json();
        deliveries.push(delivery);
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
      }
    );
    let adoptionAttempts = 0;
    const subscriptions = new FakeDurableObjectNamespace((_name, request) => {
      if (new URL(request.url).pathname === "/adopt-registration") {
        adoptionAttempts += 1;
        throw new Error("adoption unavailable");
      }

      return Promise.resolve(Response.json({ ok: true }));
    });
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: subscriptions,
    });
    const register = (
      subscriptionId: string,
      args: { mode: "broad-labels" | "partition-todos"; ownerToken: string }
    ) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/register", {
          body: JSON.stringify({
            args,
            authorizationHeader: "Bearer owner-a",
            connectionKey: "client-a",
            connectionName: "connection:0",
            epoch: 1,
            leaseExpiresAt: Date.now() + 60_000,
            queryName: "realtime:dependencyTracking",
            runtimeId,
            shardName: globalShardName,
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await register("sub-failing-state", {
      mode: "partition-todos",
      ownerToken: "owner-a",
    });
    await register("sub-stable-state", {
      mode: "broad-labels",
      ownerToken: "owner-a",
    });
    await createTodoViaRpc("owner-a", "state-update-failure");
    await createRealtimeOutboxEvent("state-update-failure", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });

    const firstResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "state-update-failure",
          shardName: globalShardName,
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const firstBody = (await firstResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(firstBody).toEqual({ evaluated: 2, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
    expect(
      getRealtimeDeliveryItems(deliveries[0]).map((item) => item.subscriptionId)
    ).toEqual(["sub-failing-state", "sub-stable-state"]);
    expect(adoptionAttempts).toBe(1);
    expect(errorLog).toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        connectionKey: "client-a",
        errorName: "Error",
        event: "runtime.realtime_registration_state_update_failed",
        queryName: "realtime:dependencyTracking",
        subscriptionId: "sub-failing-state",
      })
    );
    expect(info).not.toHaveBeenCalledWith(
      "baseflare-runtime",
      expect.objectContaining({
        event: "runtime.metric",
        metric: "baseflare.runtime.realtime.delivery_batches",
        tags: { result: "undelivered" },
      })
    );

    await createRealtimeOutboxEvent("stable-labels-unchanged", Date.now(), {
      tables: ["labels"],
    });
    const secondResponse = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "stable-labels-unchanged",
          shardName: globalShardName,
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const secondBody = (await secondResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(secondBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
  });

  it("runs realtime delivered registration bookkeeping concurrently", async () => {
    const runtimeId = createRealtimeRuntimeId();
    await env.APP_DB.prepare(
      "UPDATE _bf_realtime_shard_generations SET subscription_shard_count = 8 WHERE generation_id = 1"
    ).run();
    const globalShardName = getRealtimeSubscriptionShardName(
      createRealtimeGlobalSubscriptionRouteTarget(),
      8
    );
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const delivery = await request.json();
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
      }
    );
    const adoptionGates: ReturnType<typeof createDeferred<void>>[] = [];
    const subscriptions = new FakeDurableObjectNamespace(
      async (_name, request) => {
        if (new URL(request.url).pathname === "/adopt-registration") {
          const gate = createDeferred<void>();
          adoptionGates.push(gate);
          await gate.promise;
        }

        return Response.json({ ok: true });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: subscriptions,
    });
    const register = (subscriptionId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/register", {
          body: JSON.stringify({
            args: { mode: "partition-todos", ownerToken: "owner-a" },
            authorizationHeader: "Bearer owner-a",
            connectionKey: "client-a",
            connectionName: "connection:0",
            epoch: 1,
            leaseExpiresAt: Date.now() + 60_000,
            queryName: "realtime:dependencyTracking",
            runtimeId,
            shardName: globalShardName,
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await register("sub-a");
    await register("sub-b");
    await createTodoViaRpc("owner-a", "parallel-bookkeeping");
    await createRealtimeOutboxEvent("parallel-bookkeeping-event", Date.now(), {
      partitions: [todoOwnerPartition("owner-a")],
      tables: ["todos"],
    });

    const responsePromise = subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "parallel-bookkeeping-event",
          shardName: globalShardName,
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await waitFor(() => adoptionGates.length >= 2);
    expect(adoptionGates).toHaveLength(2);
    for (const gate of adoptionGates) {
      gate.resolve(undefined);
    }
    const response = await responsePromise;
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(body).toEqual({ evaluated: 2, failed: 0, ok: true });
  });

  it("deduplicates concurrent realtime re-evaluation for the same registration", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveryGate = createDeferred();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const delivery = await request.json();
        deliveries.push(delivery);
        await deliveryGate.promise;
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
    await createTodoViaRpc("owner-a", "dedupe-concurrent-notify");
    await createRealtimeOutboxEvent("dedupe-concurrent-notify-event");

    const notify = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId: "dedupe-concurrent-notify-event" }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    const firstNotify = notify();
    for (
      let attempt = 0;
      attempt < 20 && connections.requests.length < 1;
      attempt += 1
    ) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const secondResponse = await notify();
    const secondBody = (await secondResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    deliveryGate.resolve();
    const firstResponse = await firstNotify;
    const firstBody = (await firstResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(secondBody).toEqual({ evaluated: 0, failed: 0, ok: true });
    expect(firstBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
  });

  it("continues concurrent realtime re-evaluation for different registrations", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveryGate = createDeferred();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const delivery = await request.json();
        deliveries.push(delivery);
        if (
          getRealtimeDeliveryItems(delivery).some(
            (item) => item.subscriptionId === "sub-a"
          )
        ) {
          await deliveryGate.promise;
        }
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (subscriptionId: string) =>
      subscriptionDo.fetch(
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
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );
    await register("sub-a");
    await register("sub-b");
    await createTodoViaRpc("owner-a", "different-registration-notify");
    await createRealtimeOutboxEvent("different-registration-notify-event");

    const notify = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({
            eventId: "different-registration-notify-event",
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    const firstNotify = notify();
    for (let attempt = 0; attempt < 20 && deliveries.length < 1; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const secondResponse = await notify();
    const secondBody = (await secondResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    deliveryGate.resolve();
    const firstResponse = await firstNotify;
    const firstBody = (await firstResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(secondBody).toEqual({ evaluated: 0, failed: 0, ok: true });
    expect(firstBody).toEqual({ evaluated: 2, failed: 0, ok: true });
    expect(
      deliveries.flatMap((delivery) =>
        getRealtimeDeliveryItems(delivery).map((item) => item.subscriptionId)
      )
    ).toEqual(["sub-a", "sub-b"]);
    expect(connections.requests).toHaveLength(1);
    expect(new URL(connections.requests[0]?.request.url ?? "").pathname).toBe(
      "/deliver"
    );
  });

  it("sends separate realtime delivery batches for different connection keys", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const deliveryGates = new Map<
      string,
      ReturnType<typeof createDeferred<void>>
    >();
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const delivery = await request.json();
        deliveries.push(delivery);
        const connectionKey = (delivery as { connectionKey: string })
          .connectionKey;
        const gate = createDeferred<void>();
        deliveryGates.set(connectionKey, gate);
        await gate.promise;
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (connectionKey: string, subscriptionId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/register", {
          body: JSON.stringify({
            args: { ownerToken: "owner-a" },
            authorizationHeader: "Bearer owner-a",
            connectionKey,
            connectionName: "connection:0",
            epoch: 1,
            leaseExpiresAt: Date.now() + 60_000,
            queryName: "todos:list",
            runtimeId,
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );
    await register("client-a", "sub-a");
    await register("client-b", "sub-b");
    await createTodoViaRpc("owner-a", "separate-batches");
    await createRealtimeOutboxEvent("separate-batch-event");

    const responsePromise = subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "separate-batch-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await waitFor(() => deliveries.length >= 2);
    expect(deliveries).toHaveLength(2);
    for (const gate of deliveryGates.values()) {
      gate.resolve(undefined);
    }
    const response = await responsePromise;
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(body).toEqual({ evaluated: 2, failed: 0, ok: true });
    expect(connections.requests).toHaveLength(2);
    expect(
      deliveries.map(
        (delivery) => (delivery as { connectionKey: string }).connectionKey
      )
    ).toEqual(["client-a", "client-b"]);
  });

  it("splits large realtime delivery groups into bounded batches", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const delivery = await request.json();
        const items = getRealtimeDeliveryItems(delivery);
        deliveries.push(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (subscriptionId: string) =>
      subscriptionDo.fetch(
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
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );
    for (let index = 0; index < REALTIME_DELIVERY_BATCH_SIZE + 1; index += 1) {
      await register(`sub-${index}`);
    }
    await createTodoViaRpc("owner-a", "bounded-delivery-batches");
    await createRealtimeOutboxEvent("bounded-delivery-batches-event");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "bounded-delivery-batches-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(body).toEqual({
      evaluated: REALTIME_DELIVERY_BATCH_SIZE + 1,
      failed: 0,
      ok: true,
    });
    expect(
      deliveries.map((delivery) => getRealtimeDeliveryItems(delivery))
    ).toHaveLength(2);
    expect(getRealtimeDeliveryItems(deliveries[0])).toHaveLength(
      REALTIME_DELIVERY_BATCH_SIZE
    );
    expect(getRealtimeDeliveryItems(deliveries[1])).toHaveLength(1);
    const deliveryBatchMetrics = info.mock.calls.filter(
      ([, payload]) =>
        (payload as { metric?: string }).metric ===
        "baseflare.runtime.realtime.delivery_batches"
    );
    expect(deliveryBatchMetrics).toHaveLength(1);
    expect(deliveryBatchMetrics[0]?.[1]).toEqual(
      expect.objectContaining({
        tags: { result: "delivered" },
        value: 1,
      })
    );
  });

  it("emits one partial metric for a partially failed chunked realtime delivery group", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        const delivery = await request.json();
        const items = getRealtimeDeliveryItems(delivery);
        deliveries.push(delivery);
        if (items.length === REALTIME_DELIVERY_BATCH_SIZE) {
          return Response.json({ ok: false }, { status: 500 });
        }

        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (subscriptionId: string) =>
      subscriptionDo.fetch(
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
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );
    for (let index = 0; index < REALTIME_DELIVERY_BATCH_SIZE + 1; index += 1) {
      await register(`sub-${index}`);
    }
    await createTodoViaRpc("owner-a", "partially-failed-chunked-delivery");
    await createRealtimeOutboxEvent("partially-failed-chunked-delivery-event");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({
          eventId: "partially-failed-chunked-delivery-event",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    const deliveryBatchMetrics = info.mock.calls.filter(
      ([, payload]) =>
        (payload as { metric?: string }).metric ===
        "baseflare.runtime.realtime.delivery_batches"
    );

    expect(body).toEqual({
      evaluated: 1,
      failed: REALTIME_DELIVERY_BATCH_SIZE,
      ok: true,
    });
    expect(deliveries).toHaveLength(2);
    expect(deliveryBatchMetrics).toHaveLength(1);
    expect(deliveryBatchMetrics[0]?.[1]).toEqual(
      expect.objectContaining({
        tags: { result: "partial" },
        value: 1,
      })
    );
  });

  it("retries undelivered items from a partially acknowledged realtime batch", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    let deliveryAttempt = 0;
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        deliveryAttempt += 1;
        const delivery = await request.json();
        deliveries.push(delivery);
        return Response.json({
          delivered: 1,
          deliveredSubscriptions: deliveryAttempt === 1 ? ["sub-a"] : ["sub-b"],
          ok: true,
        });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (subscriptionId: string) =>
      subscriptionDo.fetch(
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
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );
    await register("sub-a");
    await register("sub-b");
    await createTodoViaRpc("owner-a", "partial-batch");
    await createRealtimeOutboxEvent("partial-batch-event");
    const notify = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId: "partial-batch-event" }),
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
    const registrationKey = realtimeRegistrationKey("client-a", "sub-b");
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const backedOffRegistration = indexState.registrations.get(registrationKey);

    expect(firstBody).toEqual({ evaluated: 1, failed: 1, ok: true });
    expect(secondBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(backedOffRegistration?.reEvaluationRetryAt).toBeGreaterThan(
      Date.now()
    );

    if (backedOffRegistration) {
      backedOffRegistration.reEvaluationRetryAt = Date.now() - 1;
    }
    const retriedResponse = await notify();
    const retriedBody = (await retriedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(retriedBody).toEqual({ evaluated: 2, failed: 0, ok: true });
    expect(backedOffRegistration?.reEvaluationRetryAt).toBeUndefined();
    expect(
      deliveries.map((delivery) =>
        getRealtimeDeliveryItems(delivery).map((item) => item.subscriptionId)
      )
    ).toEqual([["sub-a", "sub-b"], ["sub-b"]]);
  });

  it("counts fully undelivered accepted realtime batches as failed", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        deliveries.push(await request.json());
        return Response.json({
          delivered: 0,
          deliveredSubscriptions: [],
          ok: true,
        });
      }
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (subscriptionId: string) =>
      subscriptionDo.fetch(
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
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );
    await register("sub-a");
    await register("sub-b");
    await createTodoViaRpc("owner-a", "undelivered-batch");
    await createRealtimeOutboxEvent("undelivered-batch-event");

    const notify = () =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId: "undelivered-batch-event" }),
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
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const registrations = ["sub-a", "sub-b"].map((subscriptionId) =>
      indexState.registrations.get(
        realtimeRegistrationKey("client-a", subscriptionId)
      )
    );

    expect(firstBody).toEqual({ evaluated: 0, failed: 2, ok: true });
    expect(secondBody).toEqual({ evaluated: 0, failed: 2, ok: true });
    for (const registration of registrations) {
      expect(registration?.reEvaluationRetryAt).toBeUndefined();
    }
    expect(deliveries).toHaveLength(2);

    const retriedResponse = await notify();
    const retriedBody = (await retriedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(retriedBody).toEqual({ evaluated: 0, failed: 2, ok: true });
    expect(deliveries).toHaveLength(3);
  });

  it("does not scale up from no-target realtime delivery batches", async () => {
    const now = vi.spyOn(Date, "now");
    const startedAt = 5_000_000;
    now.mockReturnValue(startedAt);
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace(() =>
      Promise.resolve(
        Response.json({ delivered: 0, deliveredSubscriptions: [], ok: true })
      )
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
          leaseExpiresAt: startedAt + REALTIME_SCALE_UP_WINDOW_MS * 2,
          queryName: "todos:list",
          runtimeId,
          subscriptionId: "sub-1",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "no-target-autoscale");
    await createRealtimeOutboxEvent("no-target-autoscale-first");
    await createRealtimeOutboxEvent("no-target-autoscale-second");
    const notify = (eventId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await notify("no-target-autoscale-first");
    const registrationKey = realtimeRegistrationKey("client-a", "sub-1");
    const registration =
      getRealtimeIndexTestState(subscriptionDo).registrations.get(
        registrationKey
      );
    if (registration) {
      registration.reEvaluationRetryAt =
        startedAt + REALTIME_SCALE_UP_WINDOW_MS;
    }
    now.mockReturnValue(startedAt + REALTIME_SCALE_UP_WINDOW_MS + 1);
    await notify("no-target-autoscale-second");

    const activeGeneration = await env.APP_DB.prepare(
      "SELECT generation_id, subscription_shard_count, status FROM _bf_realtime_shard_generations WHERE status = 'active'"
    ).first<{
      generation_id: number;
      status: string;
      subscription_shard_count: number;
    }>();

    expect(activeGeneration).toEqual({
      generation_id: 1,
      status: "active",
      subscription_shard_count: 1,
    });
    expect(connections.requests).toHaveLength(2);
  });

  it("keeps realtime deliveries retryable when item acknowledgements are malformed", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const deliveries: unknown[] = [];
    let deliveryAttempt = 0;
    const connections = new FakeDurableObjectNamespace(
      async (_name, request) => {
        deliveryAttempt += 1;
        const delivery = await request.json();
        deliveries.push(delivery);
        if (deliveryAttempt === 1) {
          return Response.json({ delivered: 1, ok: true });
        }

        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
          subscriptionId: "sub-a",
        }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    await createTodoViaRpc("owner-a", "malformed-ack");
    await createRealtimeOutboxEvent("malformed-ack-first");
    await createRealtimeOutboxEvent("malformed-ack-second");
    const notify = (eventId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/notify", {
          body: JSON.stringify({ eventId }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );

    await notify("malformed-ack-first");
    const registrationKey = realtimeRegistrationKey("client-a", "sub-a");
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const backedOffRegistration = indexState.registrations.get(registrationKey);
    expect(backedOffRegistration?.reEvaluationRetryAt).toBeGreaterThan(
      Date.now()
    );

    if (backedOffRegistration) {
      backedOffRegistration.reEvaluationRetryAt = Date.now() - 1;
    }
    await notify("malformed-ack-second");

    expect(
      deliveries.map((delivery) =>
        getRealtimeDeliveryItems(delivery).map((item) => item.subscriptionId)
      )
    ).toEqual([["sub-a"], ["sub-a"]]);
  });

  it("bounds realtime re-evaluation concurrency", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace((_name, request) =>
      acknowledgeRealtimeDeliveryRequest(request)
    );
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    const register = (subscriptionId: string) =>
      subscriptionDo.fetch(
        new Request("https://baseflare.internal/register", {
          body: JSON.stringify({
            args: { ownerToken: "owner-a" },
            authorizationHeader: "Bearer owner-a",
            connectionKey: "client-a",
            connectionName: "connection:0",
            epoch: 1,
            leaseExpiresAt: Date.now() + 60_000,
            queryName: "realtime:concurrencyTracking",
            runtimeId,
            subscriptionId,
          }),
          headers: { "content-type": "application/json" },
          method: "POST",
        })
      );
    for (let index = 0; index < 12; index += 1) {
      await register(`sub-${index}`);
    }
    await createTodoViaRpc("owner-a", "bounded-re-evaluation");
    await createRealtimeOutboxEvent("bounded-re-evaluation-event");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "bounded-re-evaluation-event" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(body).toEqual({ evaluated: 12, failed: 0, ok: true });
    expect(maxActiveRealtimeConcurrencyQueries).toBeGreaterThan(1);
    expect(maxActiveRealtimeConcurrencyQueries).toBeLessThanOrEqual(8);
  });

  it("renews expired realtime leases after successful delivery", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace((_name, request) =>
      acknowledgeRealtimeDeliveryRequest(request)
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

  it("retries realtime delivery when no socket receives the result without backoff", async () => {
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
    const registrationKey = realtimeRegistrationKey("client-a", "sub-1");
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const registration = indexState.registrations.get(registrationKey);

    expect(firstBody).toEqual({ evaluated: 0, failed: 1, ok: true });
    expect(secondBody).toEqual({ evaluated: 0, failed: 1, ok: true });
    expect(registration?.reEvaluationRetryAt).toBeUndefined();
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

    expect(body).toEqual({ evaluated: 0, failed: 1, ok: true });
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

        const delivery = await request.json();
        deliveries.push(delivery);
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
    const registrationKey = realtimeRegistrationKey("client-a", "sub-1");
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const failedRegistration = indexState.registrations.get(registrationKey);
    expect(failedRegistration?.reEvaluationRetryAt).toBeGreaterThan(Date.now());

    const skippedResponse = await notify();
    const skippedBody = (await skippedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(skippedBody).toEqual({ evaluated: 0, failed: 0, ok: true });
    expect(deliveryAttempts).toBe(1);
    expect(deliveries).toHaveLength(0);

    if (failedRegistration) {
      failedRegistration.reEvaluationRetryAt = Date.now() - 1;
    }
    const retriedResponse = await notify();
    const retriedBody = (await retriedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    expect(retriedBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
    expect(
      getFirstRealtimeDelivery(deliveries[0]).result.map((todo) => todo.text)
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

        const delivery = await request.json();
        deliveries.push(delivery);
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
    const registrationKey = realtimeRegistrationKey("client-a", "sub-1");
    const indexState = getRealtimeIndexTestState(subscriptionDo);
    const failedRegistration = indexState.registrations.get(registrationKey);
    expect(failedRegistration?.reEvaluationRetryAt).toBeGreaterThan(Date.now());

    const skippedResponse = await notify();
    const skippedBody = (await skippedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(skippedBody).toEqual({ evaluated: 0, failed: 0, ok: true });
    expect(deliveryAttempts).toBe(1);
    expect(deliveries).toHaveLength(0);

    if (failedRegistration) {
      failedRegistration.reEvaluationRetryAt = Date.now() - 1;
    }
    const retriedResponse = await notify();
    const retriedBody = (await retriedResponse.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };
    expect(retriedBody).toEqual({ evaluated: 1, failed: 0, ok: true });
    expect(deliveries).toHaveLength(1);
    expect(
      getFirstRealtimeDelivery(deliveries[0]).result.map((todo) => todo.text)
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
      registrations: Array<{
        reEvaluationRetryAt?: number;
        subscriptionId: string;
      }>;
    };

    expect(body).toEqual({ evaluated: 0, failed: 1, ok: true });
    expect(
      registrationsBody.registrations.map(
        (registration) => registration.subscriptionId
      )
    ).toEqual(["sub-1"]);
    expect(
      registrationsBody.registrations[0]?.reEvaluationRetryAt
    ).toBeGreaterThan(Date.now());
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
        const delivery = await request.json();
        deliveries.push(delivery);
        const items = getRealtimeDeliveryItems(delivery);
        return Response.json({
          delivered: items.length,
          deliveredSubscriptions: items.map((item) => item.subscriptionId),
          ok: true,
        });
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
      getFirstRealtimeDelivery(deliveries[0]).result.map((todo) => todo.text)
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
