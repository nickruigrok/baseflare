import { generateId } from "baseflare/values";
import type { QueryDefinition } from "../functions/types";
import type { Rules } from "../permissions/types";
import type { Schema } from "../schema/types";
import { REALTIME_OUTBOX_TABLE_NAME } from "../schema/types";

import { bindStatement } from "./d1";
import { InternalRuntimeError, ValidationRuntimeError } from "./errors";
import { executeQueryDefinition } from "./execution";
import type { FunctionIndex } from "./function-index";
import { logRuntimeEvent } from "./logging";
import type {
  BaseflareExecutionContext,
  BaseflareRuntimeEnv,
  D1Database,
  DurableObjectNamespace,
  RuntimeDatabase,
} from "./types";

declare const WebSocketPair: {
  new (): { readonly 0: WebSocket; readonly 1: RuntimeWebSocket };
};

type RuntimeWebSocket = WebSocket & {
  accept(): void;
};

export interface RealtimePartitionTarget {
  readonly partitionKey: string;
  readonly partitionValue: string;
  readonly tableName: string;
}

export interface RealtimeOutboxEvent {
  readonly eventId: string;
  readonly partitions: readonly RealtimePartitionTarget[];
  readonly tables: readonly string[];
}

export interface RealtimeMutationNotifier {
  readonly enabled: true;
  notify(events: readonly RealtimeOutboxEvent[]): void;
}

export interface RealtimeOutboxOperation {
  readonly event: RealtimeOutboxEvent;
  readonly expectedChanges: number;
  readonly params: readonly (string | number | null)[];
  readonly sql: string;
  readonly type: "insert-realtime-outbox";
}

interface RealtimeRegistration {
  readonly args: unknown;
  readonly authorizationHeader?: string;
  readonly connectionKey: string;
  readonly connectionName: string;
  readonly epoch: number;
  readonly leaseExpiresAt: number;
  readonly queryName: string;
  readonly subscriptionId: string;
}

interface RealtimeObjectEnv extends BaseflareRuntimeEnv {
  REALTIME_CONNECTIONS: DurableObjectNamespace;
  REALTIME_SUBSCRIPTIONS: DurableObjectNamespace;
}

interface RealtimeRuntime {
  readonly functionIndex: FunctionIndex;
  readonly rules?: Rules;
  readonly schema: Schema;
}

type StoredRealtimeRegistration = RealtimeRegistration & {
  lastResultJson?: string;
};

const DEFAULT_REALTIME_SHARD_COUNT = 1;
const REALTIME_LEASE_MS = 60_000;
const JSON_HEADERS = { "content-type": "application/json" } as const;
let configuredRealtimeRuntime: RealtimeRuntime | undefined;

export function configureRealtimeRuntime(runtime: RealtimeRuntime): void {
  configuredRealtimeRuntime = runtime;
}

function jsonResponse(value: unknown, init: ResponseInit = {}): Response {
  return Response.json(value, {
    ...init,
    headers: { ...JSON_HEADERS, ...init.headers },
  });
}

function parseObject(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new ValidationRuntimeError(`${label} must be an object`);
  }

  return value as Record<string, unknown>;
}

async function readJsonObject(
  request: Request
): Promise<Record<string, unknown>> {
  try {
    return parseObject((await request.json()) as unknown, "Realtime message");
  } catch (error) {
    if (error instanceof ValidationRuntimeError) {
      throw error;
    }

    throw new ValidationRuntimeError("Realtime message JSON is malformed");
  }
}

function getStringField(
  object: Record<string, unknown>,
  fieldName: string
): string {
  const value = object[fieldName];
  if (typeof value !== "string" || value.length === 0) {
    throw new ValidationRuntimeError(
      `Realtime field "${fieldName}" must be a non-empty string`
    );
  }

  return value;
}

function getEpoch(value: unknown): number {
  if (value === undefined) {
    return 0;
  }

  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new ValidationRuntimeError(
      'Realtime field "epoch" must be a non-negative integer'
    );
  }

  return value;
}

function getRealtimeShardName(prefix: string, key: string): string {
  let hash = 0;
  for (const char of key) {
    hash = (hash * 31 + char.charCodeAt(0)) % Number.MAX_SAFE_INTEGER;
  }

  return `${prefix}:${hash % DEFAULT_REALTIME_SHARD_COUNT}`;
}

export function getRealtimeConnectionShardName(key: string): string {
  return getRealtimeShardName("connection", key);
}

export function getRealtimeSubscriptionShardName(): string {
  // Phase 3A keeps subscription routing singleton-shaped. Dependency-sharded
  // subscription fanout will replace this helper when N > 1 is implemented.
  return "subscription:0";
}

export function createRealtimeOutboxOperation(
  event: RealtimeOutboxEvent,
  expectedPreviousChanges: number
): RealtimeOutboxOperation {
  return {
    event,
    expectedChanges: 1,
    params: [
      event.eventId,
      Date.now(),
      JSON.stringify(event.tables),
      JSON.stringify(event.partitions),
      expectedPreviousChanges,
    ],
    sql: `INSERT INTO ${REALTIME_OUTBOX_TABLE_NAME} (event_id, created_at, tables, partitions)
          SELECT ?, ?, ?, ? WHERE changes() = ?`,
    type: "insert-realtime-outbox",
  };
}

export function createRealtimeOutboxEvent(
  tableNames: readonly string[],
  partitions: readonly RealtimePartitionTarget[]
): RealtimeOutboxEvent {
  return {
    eventId: generateId(),
    partitions: [...partitions].sort((left, right) =>
      `${left.tableName}/${left.partitionKey}/${left.partitionValue}`.localeCompare(
        `${right.tableName}/${right.partitionKey}/${right.partitionValue}`
      )
    ),
    tables: [...tableNames].sort(),
  };
}

export function createRealtimeMutationNotifier(
  env: BaseflareRuntimeEnv,
  ctx: BaseflareExecutionContext
): RealtimeMutationNotifier | undefined {
  if (!env.REALTIME_SUBSCRIPTIONS) {
    return undefined;
  }

  return {
    enabled: true,
    notify(events) {
      for (const event of events) {
        const shardName = getRealtimeSubscriptionShardName();
        const stub = env.REALTIME_SUBSCRIPTIONS?.get(
          env.REALTIME_SUBSCRIPTIONS.idFromName(shardName)
        );
        if (!stub) {
          continue;
        }

        ctx.waitUntil(
          stub
            .fetch("https://baseflare.internal/notify", {
              body: JSON.stringify({ eventId: event.eventId }),
              headers: JSON_HEADERS,
              method: "POST",
            })
            .catch((error: unknown) => {
              logRuntimeEvent("error", "runtime.realtime_notify_failed", {
                errorName: error instanceof Error ? error.name : typeof error,
                eventId: event.eventId,
              });
            })
        );
      }
    },
  };
}

export async function fetchRealtimeOutboxEvents(
  database: Pick<RuntimeDatabase, "prepare">,
  afterEventId: string | null,
  limit: number
): Promise<
  Array<{
    readonly eventId: string;
    readonly partitions: readonly RealtimePartitionTarget[];
    readonly tables: readonly string[];
  }>
> {
  const boundedLimit = Math.min(Math.max(limit, 1), 1000);
  const sql =
    afterEventId === null
      ? `SELECT event_id, tables, partitions FROM ${REALTIME_OUTBOX_TABLE_NAME} ORDER BY event_id ASC LIMIT ?`
      : `SELECT event_id, tables, partitions FROM ${REALTIME_OUTBOX_TABLE_NAME} WHERE event_id > ? ORDER BY event_id ASC LIMIT ?`;
  const params =
    afterEventId === null ? [boundedLimit] : [afterEventId, boundedLimit];
  const result = await bindStatement(database, sql, params).all<{
    event_id: string;
    partitions: string;
    tables: string;
  }>();

  return (result.results ?? []).map((row) => ({
    eventId: row.event_id,
    partitions: JSON.parse(row.partitions) as RealtimePartitionTarget[],
    tables: JSON.parse(row.tables) as string[],
  }));
}

export async function routeRealtimeSubscribe(
  request: Request,
  env: BaseflareRuntimeEnv
): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== "/api/subscribe") {
    return null;
  }

  if (request.method !== "GET") {
    throw new ValidationRuntimeError(
      "Realtime subscription requests must use GET"
    );
  }

  if (!env.REALTIME_CONNECTIONS) {
    throw new InternalRuntimeError(
      "Baseflare runtime misconfiguration: REALTIME_CONNECTIONS Durable Object binding is required for realtime subscriptions"
    );
  }

  const clientKey =
    url.searchParams.get("clientId") ??
    url.searchParams.get("sessionId") ??
    "default";
  const shardName = getRealtimeConnectionShardName(clientKey);
  const stub = env.REALTIME_CONNECTIONS.get(
    env.REALTIME_CONNECTIONS.idFromName(shardName)
  );

  return await stub.fetch(request);
}

export class RealtimeConnectionDO {
  private readonly env: RealtimeObjectEnv;
  private readonly socketAuthorizationHeaders = new Map<
    RuntimeWebSocket,
    string
  >();
  private readonly socketConnectionKeys = new Map<RuntimeWebSocket, string>();
  private readonly socketConnectionNames = new Map<RuntimeWebSocket, string>();
  private readonly socketsByConnectionKey = new Map<
    string,
    Set<RuntimeWebSocket>
  >();
  private readonly sockets = new Set<RuntimeWebSocket>();

  constructor(_state: unknown, env: RealtimeObjectEnv) {
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "GET") {
      return this.acceptWebSocket(request);
    }

    if (request.method === "POST" && url.pathname === "/deliver") {
      const message = await readJsonObject(request);
      const delivered = this.deliver(message);
      return jsonResponse({ delivered, ok: true });
    }

    if (request.method === "POST" && url.pathname === "/reconcile") {
      return jsonResponse({ ok: true });
    }

    return new Response("Not found", { status: 404 });
  }

  private acceptWebSocket(request: Request): Response {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      throw new ValidationRuntimeError(
        "Realtime subscription requests must upgrade to WebSocket"
      );
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    const url = new URL(request.url);
    const clientKey =
      url.searchParams.get("clientId") ??
      url.searchParams.get("sessionId") ??
      "default";
    server.accept();
    this.addSocket(server, clientKey);
    this.socketConnectionNames.set(
      server,
      getRealtimeConnectionShardName(clientKey)
    );
    const authorizationHeader = request.headers.get("authorization");
    if (authorizationHeader) {
      this.socketAuthorizationHeaders.set(server, authorizationHeader);
    }
    server.addEventListener("message", (event: MessageEvent) => {
      this.handleMessage(server, event.data).catch((error: unknown) => {
        server.send(
          JSON.stringify({
            error:
              error instanceof Error
                ? error.message
                : "Realtime message failed",
            type: "error",
          })
        );
      });
    });
    server.addEventListener("close", () => {
      this.removeSocket(server);
      this.socketAuthorizationHeaders.delete(server);
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    } as ResponseInit);
  }

  private async handleMessage(
    socket: RuntimeWebSocket,
    data: unknown
  ): Promise<void> {
    if (typeof data !== "string") {
      throw new ValidationRuntimeError("Realtime messages must be text JSON");
    }

    const message = parseObject(
      JSON.parse(data) as unknown,
      "Realtime message"
    );
    const type = getStringField(message, "type");
    if (type === "subscribe") {
      await this.registerSubscription(message, socket);
      return;
    }

    if (type === "unsubscribe") {
      await this.unregisterSubscription(message, socket);
      return;
    }

    if (type === "restore") {
      await this.restoreSubscriptions(message, socket);
      return;
    }

    throw new ValidationRuntimeError(`Unknown realtime message type "${type}"`);
  }

  private async registerSubscription(
    message: Record<string, unknown>,
    socket: RuntimeWebSocket
  ): Promise<void> {
    const registration = this.createRegistration(message, socket);
    await this.subscriptionStub().fetch("https://baseflare.internal/register", {
      body: JSON.stringify(registration),
      headers: JSON_HEADERS,
      method: "POST",
    });
    socket.send(
      JSON.stringify({
        subscriptionId: registration.subscriptionId,
        type: "subscribed",
      })
    );
  }

  private async unregisterSubscription(
    message: Record<string, unknown>,
    socket: RuntimeWebSocket
  ): Promise<void> {
    const subscriptionId = getStringField(message, "subscriptionId");
    await this.subscriptionStub().fetch(
      "https://baseflare.internal/unregister",
      {
        body: JSON.stringify({ subscriptionId }),
        headers: JSON_HEADERS,
        method: "POST",
      }
    );
    socket.send(JSON.stringify({ subscriptionId, type: "unsubscribed" }));
  }

  private async restoreSubscriptions(
    message: Record<string, unknown>,
    socket: RuntimeWebSocket
  ): Promise<void> {
    const subscriptions = message.subscriptions;
    if (!Array.isArray(subscriptions)) {
      throw new ValidationRuntimeError(
        'Realtime field "subscriptions" must be an array'
      );
    }

    for (const subscription of subscriptions) {
      await this.registerSubscription(
        parseObject(subscription, "Realtime subscription"),
        socket
      );
    }
    socket.send(JSON.stringify({ type: "restored" }));
  }

  private createRegistration(
    message: Record<string, unknown>,
    socket?: RuntimeWebSocket
  ): RealtimeRegistration {
    const subscriptionId = getStringField(message, "subscriptionId");
    return {
      args: message.args ?? {},
      authorizationHeader: socket
        ? this.socketAuthorizationHeaders.get(socket)
        : undefined,
      connectionKey: socket
        ? (this.socketConnectionKeys.get(socket) ?? "default")
        : "default",
      connectionName: socket
        ? (this.socketConnectionNames.get(socket) ?? "connection:0")
        : "connection:0",
      epoch: getEpoch(message.epoch),
      leaseExpiresAt: Date.now() + REALTIME_LEASE_MS,
      queryName: getStringField(message, "queryName"),
      subscriptionId,
    };
  }

  private addSocket(socket: RuntimeWebSocket, connectionKey: string): void {
    this.sockets.add(socket);
    this.socketConnectionKeys.set(socket, connectionKey);
    const sockets =
      this.socketsByConnectionKey.get(connectionKey) ??
      new Set<RuntimeWebSocket>();
    sockets.add(socket);
    this.socketsByConnectionKey.set(connectionKey, sockets);
  }

  private removeSocket(socket: RuntimeWebSocket): void {
    this.sockets.delete(socket);
    const connectionKey = this.socketConnectionKeys.get(socket);
    this.socketConnectionKeys.delete(socket);
    this.socketConnectionNames.delete(socket);
    if (!connectionKey) {
      return;
    }

    const sockets = this.socketsByConnectionKey.get(connectionKey);
    sockets?.delete(socket);
    if (sockets?.size === 0) {
      this.socketsByConnectionKey.delete(connectionKey);
    }
  }

  private deliver(message: Record<string, unknown>): number {
    const connectionKey = getStringField(message, "connectionKey");
    const { connectionKey: _connectionKey, ...deliveryMessage } = message;
    const payload = JSON.stringify({
      message: deliveryMessage,
      type: "delivery",
    });
    const sockets = this.socketsByConnectionKey.get(connectionKey);
    if (!sockets) {
      return 0;
    }

    let delivered = 0;
    for (const socket of sockets) {
      socket.send(payload);
      delivered += 1;
    }

    return delivered;
  }

  private subscriptionStub() {
    const shardName = getRealtimeSubscriptionShardName();
    return this.env.REALTIME_SUBSCRIPTIONS.get(
      this.env.REALTIME_SUBSCRIPTIONS.idFromName(shardName)
    );
  }
}

export class RealtimeSubscriptionDO {
  private readonly database: D1Database;
  private readonly env: RealtimeObjectEnv;
  private readonly registrations = new Map<
    string,
    StoredRealtimeRegistration
  >();
  private lastSeenEventId: string | null = null;

  constructor(_state: unknown, env: RealtimeObjectEnv) {
    this.database = env.APP_DB;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== "POST") {
      return new Response("Not found", { status: 404 });
    }

    if (url.pathname === "/register") {
      const registration = this.parseRegistration(
        await readJsonObject(request)
      );
      const existing = this.registrations.get(registration.subscriptionId);
      if (!existing || registration.epoch >= existing.epoch) {
        this.registrations.set(registration.subscriptionId, registration);
      }
      return jsonResponse({ ok: true });
    }

    if (url.pathname === "/unregister") {
      const body = await readJsonObject(request);
      this.registrations.delete(getStringField(body, "subscriptionId"));
      return jsonResponse({ ok: true });
    }

    if (url.pathname === "/notify") {
      const body = await readJsonObject(request);
      this.lastSeenEventId = getStringField(body, "eventId");
      await this.reEvaluateActiveRegistrations();
      return jsonResponse({ ok: true });
    }

    if (url.pathname === "/catch-up") {
      const body = await readJsonObject(request);
      const afterEventId =
        typeof body.afterEventId === "string" ? body.afterEventId : null;
      const events = await fetchRealtimeOutboxEvents(
        this.database,
        afterEventId,
        typeof body.limit === "number" ? body.limit : 100
      );
      this.lastSeenEventId = events.at(-1)?.eventId ?? this.lastSeenEventId;
      return jsonResponse({ events, ok: true });
    }

    if (url.pathname === "/registrations") {
      return jsonResponse({
        lastSeenEventId: this.lastSeenEventId,
        registrations: this.getActiveRegistrations(),
      });
    }

    return new Response("Not found", { status: 404 });
  }

  private parseRegistration(
    body: Record<string, unknown>
  ): RealtimeRegistration {
    return {
      args: body.args ?? {},
      authorizationHeader:
        typeof body.authorizationHeader === "string"
          ? body.authorizationHeader
          : undefined,
      connectionKey: getStringField(body, "connectionKey"),
      connectionName: getStringField(body, "connectionName"),
      epoch: getEpoch(body.epoch),
      leaseExpiresAt:
        typeof body.leaseExpiresAt === "number"
          ? body.leaseExpiresAt
          : Date.now() + REALTIME_LEASE_MS,
      queryName: getStringField(body, "queryName"),
      subscriptionId: getStringField(body, "subscriptionId"),
    };
  }

  private getActiveRegistrations(): StoredRealtimeRegistration[] {
    const now = Date.now();
    const active: RealtimeRegistration[] = [];
    for (const [subscriptionId, registration] of this.registrations) {
      if (registration.leaseExpiresAt <= now) {
        this.registrations.delete(subscriptionId);
        continue;
      }

      active.push(registration);
    }

    return active;
  }

  private async reEvaluateActiveRegistrations(): Promise<void> {
    for (const registration of this.getActiveRegistrations()) {
      await this.reEvaluateRegistration(registration);
    }
  }

  private async reEvaluateRegistration(
    registration: StoredRealtimeRegistration
  ): Promise<void> {
    const runtime = configuredRealtimeRuntime;
    if (!runtime) {
      throw new InternalRuntimeError(
        "Baseflare runtime misconfiguration: realtime query runtime is not configured"
      );
    }

    const entry = runtime.functionIndex.getByName(
      "query",
      registration.queryName,
      "public"
    );
    if (!entry) {
      throw new ValidationRuntimeError(
        `Realtime query "${registration.queryName}" was not found`
      );
    }

    const headers = new Headers();
    if (registration.authorizationHeader) {
      headers.set("authorization", registration.authorizationHeader);
    }

    const result = await executeQueryDefinition(
      entry.definition as QueryDefinition,
      {
        database: this.database,
        executionContext: {
          waitUntil() {
            // Realtime DO query execution does not schedule nested background work.
          },
        },
        functionIndex: runtime.functionIndex,
        requestHeaders: headers,
        rules: runtime.rules,
        schema: runtime.schema,
      },
      registration.args
    );
    const resultJson = JSON.stringify(result);
    if (resultJson === registration.lastResultJson) {
      return;
    }

    registration.lastResultJson = resultJson;
    await this.env.REALTIME_CONNECTIONS.get(
      this.env.REALTIME_CONNECTIONS.idFromName(registration.connectionName)
    ).fetch("https://baseflare.internal/deliver", {
      body: JSON.stringify({
        connectionKey: registration.connectionKey,
        result,
        subscriptionId: registration.subscriptionId,
      }),
      headers: JSON_HEADERS,
      method: "POST",
    });
  }
}
