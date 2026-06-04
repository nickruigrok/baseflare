import { env } from "cloudflare:test";
import { v } from "baseflare/values";
import {
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { query } from "../functions/query";
import { defineRules } from "../permissions/define-rules";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { createFunctionIndex } from "./function-index";
import { buildBaseflareManifest } from "./manifest";
import {
  configureRealtimeRuntime,
  resetRealtimeRuntimeStateForTest,
} from "./realtime/shared";
import { RealtimeSubscriptionDO } from "./realtime/subscription-do";
import {
  REALTIME_CATCH_UP_EVENT_LIMIT,
  REALTIME_DELIVERY_BATCH_SIZE,
  REALTIME_REEVALUATION_CONCURRENCY,
} from "./realtime/types";
import { applyRuntimeSchema } from "./schema-apply";
import type {
  BaseflareManifest,
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
  readonly requests: Request[] = [];
  private readonly handler: (request: Request) => Promise<Response>;

  constructor(handler?: (request: Request) => Promise<Response>) {
    this.handler =
      handler ?? (() => Promise.resolve(Response.json({ ok: true })));
  }

  get(_id: DurableObjectId): DurableObjectStub {
    return {
      fetch: async (input, init) => {
        const request =
          input instanceof Request ? input : new Request(input, init);
        this.requests.push(request);
        return await this.handler(request);
      },
    };
  }

  idFromName(name: string): DurableObjectId {
    return { name };
  }
}

const schema = defineSchema({
  todos: defineTable({
    ownerToken: v.string(),
    text: v.string(),
  }).index("by_owner", ["ownerToken"]),
});

const rules = defineRules({
  todos: {
    read: () => true,
  },
});

let realtimeDependencyTrackingQueryCalls = 0;
let slowActiveQueries = 0;
let slowMaxActiveQueries = 0;

const realtimeDependencyTrackingQuery = query({
  args: { ownerToken: v.string() },
  handler(ctx, args) {
    realtimeDependencyTrackingQueryCalls += 1;
    return ctx.db
      .query("todos")
      .filter({ ownerToken: args.ownerToken })
      .collect();
  },
});

const slowRealtimeQuery = query({
  args: { ownerToken: v.string() },
  async handler(ctx, args) {
    slowActiveQueries += 1;
    slowMaxActiveQueries = Math.max(slowMaxActiveQueries, slowActiveQueries);
    await new Promise((resolve) => setTimeout(resolve, 10));
    slowActiveQueries -= 1;
    return ctx.db
      .query("todos")
      .filter({ ownerToken: args.ownerToken })
      .collect();
  },
});

function createManifest(): BaseflareManifest {
  return buildBaseflareManifest({
    queries: [
      {
        definition: realtimeDependencyTrackingQuery,
        exportName: "dependency",
        modulePath: "perf",
      },
      { definition: slowRealtimeQuery, exportName: "slow", modulePath: "perf" },
    ],
    rules,
    schema,
  });
}

function createRealtimeRuntimeId(): string {
  const manifest = createManifest();
  return configureRealtimeRuntime({
    functionIndex: createFunctionIndex(manifest),
    rules: manifest.rules,
    schema: manifest.schema,
  });
}

async function createRealtimeOutboxEvent(
  eventId: string,
  ownerToken: string
): Promise<void> {
  await env.APP_DB.prepare(
    "INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions) VALUES (?, ?, ?, ?)"
  )
    .bind(
      eventId,
      Date.now() - 1000,
      JSON.stringify(["todos"]),
      JSON.stringify([
        {
          partitionKey: "by_owner",
          partitionValue: JSON.stringify([ownerToken]),
          tableName: "todos",
        },
      ])
    )
    .run();
}

async function registerSubscription(
  subscriptionDo: RealtimeSubscriptionDO,
  runtimeId: string,
  subscriptionId: string,
  queryName: string,
  ownerToken: string,
  options: { readonly shardName?: string } = {}
): Promise<void> {
  await subscriptionDo.fetch(
    new Request("https://baseflare.internal/register", {
      body: JSON.stringify({
        args: { ownerToken },
        connectionKey: "client-a",
        connectionName: "connection:0",
        epoch: 1,
        leaseExpiresAt: Date.now() + 60_000,
        queryName,
        runtimeId,
        shardName: options.shardName,
        subscriptionId,
      }),
      headers: { "content-type": "application/json" },
      method: "POST",
    })
  );
}

describe("realtime local performance gate", () => {
  beforeAll(async () => {
    await applyRuntimeSchema(env.APP_DB, schema);
  });

  beforeEach(async () => {
    resetRealtimeRuntimeStateForTest();
    realtimeDependencyTrackingQueryCalls = 0;
    slowActiveQueries = 0;
    slowMaxActiveQueries = 0;
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.spyOn(console, "info").mockImplementation(() => undefined);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await env.APP_DB.prepare("DELETE FROM todos").run();
    await env.APP_DB.prepare("DELETE FROM _bf_realtime_outbox").run();
    await env.APP_DB.prepare(
      "UPDATE _bf_table_versions SET version = 0 WHERE table_name = 'todos'"
    ).run();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("handles 25k registrations distributed across subscription shards", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const shardCount = 32;
    const registrationCount = 25_000;
    const registrationChunkSize = 500;
    const subscriptionDos = Array.from(
      { length: shardCount },
      () =>
        new RealtimeSubscriptionDO(null, {
          APP_DB: env.APP_DB,
          REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(),
          REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
        })
    );

    for (
      let startIndex = 0;
      startIndex < registrationCount;
      startIndex += registrationChunkSize
    ) {
      const endIndex = Math.min(
        startIndex + registrationChunkSize,
        registrationCount
      );
      await Promise.all(
        Array.from({ length: endIndex - startIndex }, (_value, offset) => {
          const registrationIndex = startIndex + offset;
          const shardIndex = registrationIndex % shardCount;
          return registerSubscription(
            subscriptionDos[shardIndex] as RealtimeSubscriptionDO,
            runtimeId,
            `sub-${registrationIndex}`,
            "perf:dependency",
            `owner-${shardIndex}`,
            { shardName: `subscription:g1:${shardIndex}` }
          );
        })
      );
    }

    const registrationCounts = await Promise.all(
      subscriptionDos.map(async (subscriptionDo) => {
        const response = await subscriptionDo.fetch(
          new Request("https://baseflare.internal/registrations", {
            body: "{}",
            method: "POST",
          })
        );
        const body = (await response.json()) as { registrations: unknown[] };
        return body.registrations.length;
      })
    );

    expect(registrationCounts.reduce((total, count) => total + count, 0)).toBe(
      registrationCount
    );
    expect(Math.max(...registrationCounts)).toBeLessThanOrEqual(
      Math.ceil(registrationCount / shardCount)
    );
  });

  it("skips unrelated partition subscriptions under load", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const connections = new FakeDurableObjectNamespace(async (request) => {
      const delivery = (await request.json()) as {
        deliveries: Array<{ subscriptionId: string }>;
      };
      return Response.json({
        delivered: delivery.deliveries.length,
        deliveredSubscriptions: delivery.deliveries.map(
          (item) => item.subscriptionId
        ),
        ok: true,
      });
    });
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    for (let index = 0; index < 200; index += 1) {
      await registerSubscription(
        subscriptionDo,
        runtimeId,
        `sub-${index}`,
        "perf:dependency",
        "owner-a"
      );
    }
    await createRealtimeOutboxEvent("prime-owner-a", "owner-a");
    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "prime-owner-a" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    realtimeDependencyTrackingQueryCalls = 0;
    await createRealtimeOutboxEvent("unrelated-owner-b", "owner-b");

    const response = await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "unrelated-owner-b" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );
    const body = (await response.json()) as {
      evaluated: number;
      failed: number;
      ok: boolean;
    };

    expect(body).toEqual({ evaluated: 0, failed: 0, ok: true });
    expect(realtimeDependencyTrackingQueryCalls).toBe(0);
  });

  it("keeps hot re-evaluation concurrency bounded", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: new FakeDurableObjectNamespace(async (request) => {
        const delivery = (await request.json()) as {
          deliveries: Array<{ subscriptionId: string }>;
        };
        return Response.json({
          delivered: delivery.deliveries.length,
          deliveredSubscriptions: delivery.deliveries.map(
            (item) => item.subscriptionId
          ),
          ok: true,
        });
      }),
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    for (let index = 0; index < 24; index += 1) {
      await registerSubscription(
        subscriptionDo,
        runtimeId,
        `sub-${index}`,
        "perf:slow",
        "owner-a"
      );
    }
    await createRealtimeOutboxEvent("hot-owner-a", "owner-a");

    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "hot-owner-a" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(slowMaxActiveQueries).toBeGreaterThan(1);
    expect(slowMaxActiveQueries).toBeLessThanOrEqual(
      REALTIME_REEVALUATION_CONCURRENCY
    );
  });

  it("keeps delivery batches within the configured item budget", async () => {
    const runtimeId = createRealtimeRuntimeId();
    const batchSizes: number[] = [];
    const connections = new FakeDurableObjectNamespace(async (request) => {
      const delivery = (await request.json()) as {
        deliveries: Array<{ subscriptionId: string }>;
      };
      batchSizes.push(delivery.deliveries.length);
      return Response.json({
        delivered: delivery.deliveries.length,
        deliveredSubscriptions: delivery.deliveries.map(
          (item) => item.subscriptionId
        ),
        ok: true,
      });
    });
    const subscriptionDo = new RealtimeSubscriptionDO(null, {
      APP_DB: env.APP_DB,
      REALTIME_CONNECTIONS: connections,
      REALTIME_SUBSCRIPTIONS: new FakeDurableObjectNamespace(),
    });
    for (let index = 0; index < REALTIME_DELIVERY_BATCH_SIZE + 1; index += 1) {
      await registerSubscription(
        subscriptionDo,
        runtimeId,
        `sub-${index}`,
        "perf:dependency",
        "owner-a"
      );
    }
    await createRealtimeOutboxEvent("batch-owner-a", "owner-a");

    await subscriptionDo.fetch(
      new Request("https://baseflare.internal/notify", {
        body: JSON.stringify({ eventId: "batch-owner-a" }),
        headers: { "content-type": "application/json" },
        method: "POST",
      })
    );

    expect(batchSizes).toEqual([REALTIME_DELIVERY_BATCH_SIZE, 1]);
  });

  it("keeps catch-up reads within the configured event budget", async () => {
    await env.APP_DB.prepare(`
      WITH RECURSIVE events(index_value) AS (
        SELECT 0
        UNION ALL
        SELECT index_value + 1 FROM events WHERE index_value < ${REALTIME_CATCH_UP_EVENT_LIMIT}
      )
      INSERT INTO _bf_realtime_outbox (event_id, created_at, tables, partitions)
      SELECT 'perf-catch-up-' || index_value, ?, ?, ? FROM events
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
    const body = (await response.json()) as { events: unknown[] };

    expect(body.events).toHaveLength(REALTIME_CATCH_UP_EVENT_LIMIT);
  });
});
