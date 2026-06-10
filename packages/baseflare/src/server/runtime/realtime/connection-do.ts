import { InternalRuntimeError, ValidationRuntimeError } from "../errors";
import { assertJsonBounds } from "../json-bounds";
import { logRuntimeEvent } from "../logging";
import type { BaseflareRuntimeEnv, DurableObjectStub } from "../types";
import {
  createRealtimeGlobalSubscriptionRouteTarget,
  getRealtimeConnectionShardName,
  getRealtimeShardGenerationIdFromName,
  getRealtimeSubscriptionShardName,
  getRealtimeSubscriptionShardNames,
  parseRealtimeSubscriptionShardName,
} from "./routing";
import { fetchActiveRealtimeShardGeneration } from "./shards";
import {
  configuredRealtimeRuntimes,
  createRealtimeAuthorizationFingerprint,
  createRealtimeSocketAttachment,
  emitRealtimeMetric,
  getEpoch,
  getOptionalSequence,
  getStringField,
  isRealtimeDurableObjectState,
  jsonResponse,
  parseObject,
  parseRealtimeSocketAttachment,
  readJsonObject,
  resolveRealtimeConnectionKey,
} from "./shared";
import { RealtimeSocketRegistry } from "./socket-registry";
import type {
  RealtimeDeliveryResult,
  RealtimeDurableObjectState,
  RealtimeObjectEnv,
  RealtimeRegistration,
  RealtimeShardGeneration,
  RealtimeSocketAttachment,
  RealtimeSocketSubscription,
  RealtimeSubscriptionRouteTarget,
  RuntimeWebSocket,
} from "./types";
import {
  JSON_HEADERS,
  REALTIME_CONNECTION_KEY_HEADER,
  REALTIME_LEASE_MS,
  REALTIME_MAX_ACTIVE_SUBSCRIPTIONS_PER_SOCKET,
  REALTIME_MAX_MESSAGE_BYTES,
  REALTIME_MAX_RESTORE_SUBSCRIPTIONS,
  REALTIME_RECONCILIATION_INTERVAL_MS,
  REALTIME_RECONCILIATIONS_METRIC,
  REALTIME_RESTORE_SUBSCRIPTIONS_METRIC,
  REALTIME_RUNTIME_ID_HEADER,
} from "./types";

declare const WebSocketPair: {
  new (): { readonly 0: WebSocket; readonly 1: RuntimeWebSocket };
};

const MESSAGE_SIZE_ENCODER = new TextEncoder();

interface RealtimeReconciliationFailure {
  readonly errorName: string;
  readonly message?: string;
  readonly shardName: string;
  readonly status?: number;
}

interface RealtimeReconciliationSummary {
  readonly attempted: number;
  readonly failed: readonly RealtimeReconciliationFailure[];
  readonly latestSequencesByShard: ReadonlyMap<string, number>;
  readonly succeeded: number;
}

export async function routeRealtimeSubscribe(
  request: Request,
  env: BaseflareRuntimeEnv,
  runtimeId: string
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

  const authorizationHeader = request.headers.get("authorization");
  const clientKey = await resolveRealtimeConnectionKey(url, {
    authorizationHeader,
    runtimeId,
  });
  const shardName = getRealtimeConnectionShardName(clientKey);
  const stub = env.REALTIME_CONNECTIONS.get(
    env.REALTIME_CONNECTIONS.idFromName(shardName)
  );
  const headers = new Headers(request.headers);
  headers.set(REALTIME_CONNECTION_KEY_HEADER, clientKey);
  headers.set(REALTIME_RUNTIME_ID_HEADER, runtimeId);

  return await stub.fetch(new Request(request, { headers }));
}

/**
 * Durable Object that holds client WebSockets via the Hibernation API,
 * registers subscriptions with subscription shards, delivers results to
 * sockets, and reconciles missed deliveries on alarms and wake-ups. Exported
 * through `baseflare/runtime` for the CLI-generated worker entry.
 */
export class RealtimeConnectionDO {
  private readonly env: RealtimeObjectEnv;
  // Deliberately in-memory only: restore state must not survive hibernation,
  // because a woken isolate must re-register its attached subscriptions.
  private readonly restoredAttachedSubscriptionKeys = new Set<string>();
  private readonly socketRegistry: RealtimeSocketRegistry;
  private readonly state: RealtimeDurableObjectState;
  private hasPendingHibernationRestoreRetry = false;

  constructor(
    state: RealtimeDurableObjectState | unknown,
    env: RealtimeObjectEnv
  ) {
    this.state = isRealtimeDurableObjectState(state) ? state : {};
    this.env = env;
    this.socketRegistry = new RealtimeSocketRegistry({
      onRemoveAttachment: (attachment) => {
        this.forgetRestored(attachment);
      },
    });
    this.restoreHibernatedSockets();
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "GET") {
      return await this.acceptWebSocket(request);
    }

    if (request.method === "POST" && url.pathname === "/deliver") {
      const message = await readJsonObject(request);
      const delivered = this.deliver(message);
      return jsonResponse({ ...delivered, ok: true });
    }

    if (request.method === "POST" && url.pathname === "/has-sockets") {
      const message = await readJsonObject(request);
      const connectionKey = getStringField(message, "connectionKey");
      const subscriptionId =
        typeof message.subscriptionId === "string"
          ? getStringField(message, "subscriptionId")
          : undefined;
      if (Array.isArray(message.subscriptionIds)) {
        const liveSubscriptionIds = message.subscriptionIds.filter(
          (id): id is string =>
            typeof id === "string" &&
            this.socketRegistry.hasSubscriptionSocket(connectionKey, id)
        );
        return jsonResponse({
          connected: this.socketRegistry.hasSockets(connectionKey),
          liveSubscriptionIds,
          ok: true,
        });
      }
      const connected = subscriptionId
        ? this.socketRegistry.hasSubscriptionSocket(
            connectionKey,
            subscriptionId
          )
        : this.socketRegistry.hasSockets(connectionKey);
      return jsonResponse({ connected, ok: true });
    }

    if (request.method === "POST" && url.pathname === "/subscription-moved") {
      const message = await readJsonObject(request);
      this.updateSubscriptionShardName(message);
      return jsonResponse({ ok: true });
    }

    return new Response("Not found", { status: 404 });
  }

  private async acceptWebSocket(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      throw new ValidationRuntimeError(
        "Realtime subscription requests must upgrade to WebSocket"
      );
    }

    const authorizationHeader = request.headers.get("authorization");
    const authorizationFingerprint = authorizationHeader
      ? await createRealtimeAuthorizationFingerprint(authorizationHeader)
      : undefined;
    const runtimeId = request.headers.get(REALTIME_RUNTIME_ID_HEADER);
    if (!runtimeId) {
      throw new ValidationRuntimeError(
        "Realtime subscription requests require a runtime id"
      );
    }
    const url = new URL(request.url);
    const clientKey =
      request.headers.get(REALTIME_CONNECTION_KEY_HEADER) ??
      (await resolveRealtimeConnectionKey(url, {
        authorizationHeader,
        runtimeId,
      }));

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    const attachment = createRealtimeSocketAttachment({
      authorizationFingerprint,
      connectionKey: clientKey,
      runtimeId,
    });
    if (this.state.acceptWebSocket) {
      this.state.acceptWebSocket(server);
      this.socketRegistry.add(server, attachment);
    } else {
      server.accept();
      this.socketRegistry.add(server, attachment);
      server.addEventListener("message", (event: MessageEvent) => {
        this.handleMessage(server, event.data).catch((error: unknown) => {
          this.sendSocketError(server, error);
        });
      });
      server.addEventListener("close", () => {
        this.socketRegistry.remove(server);
      });
      server.addEventListener("error", () => {
        this.socketRegistry.remove(server);
      });
    }

    return new Response(null, {
      status: 101,
      webSocket: client,
    } as ResponseInit);
  }

  async webSocketMessage(
    socket: RuntimeWebSocket,
    message: string | ArrayBuffer
  ): Promise<void> {
    try {
      await this.handleMessage(socket, message);
    } catch (error) {
      this.sendSocketError(socket, error);
    }
  }

  webSocketClose(socket: RuntimeWebSocket): void {
    this.socketRegistry.remove(socket);
  }

  webSocketError(socket: RuntimeWebSocket): void {
    this.socketRegistry.remove(socket);
  }

  async alarm(): Promise<void> {
    await this.reconcileActiveSubscriptions();
  }

  private async handleMessage(
    socket: RuntimeWebSocket,
    data: unknown
  ): Promise<void> {
    if (typeof data !== "string") {
      throw new ValidationRuntimeError("Realtime messages must be text JSON");
    }
    if (
      MESSAGE_SIZE_ENCODER.encode(data).byteLength > REALTIME_MAX_MESSAGE_BYTES
    ) {
      throw new ValidationRuntimeError(
        `Realtime messages must be at most ${REALTIME_MAX_MESSAGE_BYTES} bytes`
      );
    }

    const message = parseObject(
      JSON.parse(data) as unknown,
      "Realtime message"
    );
    assertJsonBounds(message, "Realtime message");
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
    socket: RuntimeWebSocket,
    options: {
      readonly scheduleReconciliation?: boolean;
      readonly sendAcknowledgement?: boolean;
      readonly subscriptionGeneration?: RealtimeShardGeneration;
    } = {}
  ): Promise<void> {
    const registration = this.createRegistration(message, socket);
    const attachment = this.socketRegistry.ensureAttachment(socket);
    const existingSubscription = this.socketRegistry.getSubscription(
      socket,
      registration.subscriptionId
    );
    if (
      !existingSubscription &&
      attachment.subscriptions.length >=
        REALTIME_MAX_ACTIVE_SUBSCRIPTIONS_PER_SOCKET
    ) {
      throw new ValidationRuntimeError(
        `Realtime sockets can have at most ${REALTIME_MAX_ACTIVE_SUBSCRIPTIONS_PER_SOCKET} active subscriptions`
      );
    }
    const subscriptionTarget = await this.subscriptionTarget(
      undefined,
      createRealtimeGlobalSubscriptionRouteTarget(),
      options.subscriptionGeneration
    );
    const response = await subscriptionTarget.stub.fetch(
      "https://baseflare.internal/register",
      {
        body: JSON.stringify({
          ...registration,
          shardName: subscriptionTarget.shardName,
        }),
        headers: JSON_HEADERS,
        method: "POST",
      }
    );
    if (!response.ok) {
      throw new InternalRuntimeError(
        `Realtime subscription registration failed with status ${response.status}`
      );
    }

    this.socketRegistry.addSubscription(socket, {
      args: registration.args,
      epoch: registration.epoch,
      queryName: registration.queryName,
      subscriptionShardName: subscriptionTarget.shardName,
      subscriptionId: registration.subscriptionId,
    });
    if (options.scheduleReconciliation ?? true) {
      await this.scheduleReconciliation();
    }
    if (options.sendAcknowledgement ?? true) {
      this.sendSubscriptionAcknowledgement(socket, registration.subscriptionId);
    }
  }

  private async unregisterSubscription(
    message: Record<string, unknown>,
    socket: RuntimeWebSocket
  ): Promise<void> {
    const subscriptionId = getStringField(message, "subscriptionId");
    const connectionKey =
      this.socketRegistry.ensureAttachment(socket).connectionKey;
    const existingSubscription = this.socketRegistry.getSubscription(
      socket,
      subscriptionId
    );
    const unregisterTarget = await this.subscriptionTarget(
      existingSubscription?.subscriptionShardName
    );
    const unregisterResponse = await unregisterTarget.stub.fetch(
      "https://baseflare.internal/unregister",
      {
        body: JSON.stringify({ connectionKey, subscriptionId }),
        headers: JSON_HEADERS,
        method: "POST",
      }
    );
    if (!unregisterResponse.ok) {
      throw new InternalRuntimeError(
        `Realtime subscription unregister failed with status ${unregisterResponse.status}`
      );
    }

    const removedSubscription = this.socketRegistry.removeSubscription(
      socket,
      subscriptionId
    );
    if (removedSubscription) {
      this.forgetRestored(removedSubscription.attachment, subscriptionId);
    }
    await this.scheduleReconciliation();
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
    if (subscriptions.length > REALTIME_MAX_RESTORE_SUBSCRIPTIONS) {
      emitRealtimeMetric(
        REALTIME_RESTORE_SUBSCRIPTIONS_METRIC,
        subscriptions.length,
        { result: "rejected" }
      );
      throw new ValidationRuntimeError(
        `Realtime restore can include at most ${REALTIME_MAX_RESTORE_SUBSCRIPTIONS} subscriptions to bound D1 concurrency and connection memory`
      );
    }

    const subscriptionGeneration = await fetchActiveRealtimeShardGeneration(
      this.env.APP_DB
    );
    const results = await Promise.allSettled(
      subscriptions.map(async (subscription, index) => {
        const subscriptionMessage = parseObject(
          subscription,
          "Realtime subscription"
        );
        assertJsonBounds(subscriptionMessage, "Realtime subscription");
        await this.registerSubscription(subscriptionMessage, socket, {
          scheduleReconciliation: false,
          sendAcknowledgement: false,
          subscriptionGeneration,
        });
        return {
          index,
          subscriptionId:
            typeof subscriptionMessage.subscriptionId === "string"
              ? subscriptionMessage.subscriptionId
              : undefined,
        };
      })
    );
    const failed: Array<{
      error: string;
      index: number;
      subscriptionId?: string;
    }> = results.flatMap((result, index) => {
      if (result.status === "fulfilled") {
        return [];
      }

      const subscription = subscriptions[index];
      const subscriptionId =
        subscription &&
        typeof subscription === "object" &&
        "subscriptionId" in subscription &&
        typeof subscription.subscriptionId === "string"
          ? subscription.subscriptionId
          : undefined;
      return [
        {
          error:
            result.reason instanceof Error
              ? result.reason.message
              : "Realtime subscription restore failed",
          index,
          subscriptionId,
        },
      ];
    });
    let reconciled = false;
    try {
      const afterSequence = getOptionalSequence(
        message.afterSequence,
        "afterSequence"
      );
      const restoreSession = this.env.APP_DB.withSession?.(
        "first-unconstrained"
      );
      const outboxBookmark = restoreSession?.getBookmark();
      const catchUpTargets = await this.subscriptionCatchUpTargets(
        socket,
        subscriptionGeneration
      );
      const catchUpResults = await Promise.allSettled(
        catchUpTargets.map(async (catchUpTarget) => {
          try {
            const catchUpResponse = await catchUpTarget.stub.fetch(
              "https://baseflare.internal/catch-up",
              {
                body: JSON.stringify({
                  afterSequence,
                  outboxBookmark,
                  shardName: catchUpTarget.shardName,
                }),
                headers: JSON_HEADERS,
                method: "POST",
              }
            );
            if (!catchUpResponse.ok) {
              throw new InternalRuntimeError(
                `Realtime restore catch-up failed for shard ${catchUpTarget.shardName} with status ${catchUpResponse.status}`
              );
            }
            const latestSequence =
              await this.readCatchUpLatestSequence(catchUpResponse);
            if (latestSequence !== null) {
              this.updateLatestDeliveredOutboxSequence(socket, latestSequence);
            }
          } catch (error) {
            if (
              error instanceof Error &&
              error.message.includes(catchUpTarget.shardName)
            ) {
              throw error;
            }

            throw new InternalRuntimeError(
              `Realtime restore catch-up failed for shard ${catchUpTarget.shardName}: ${
                error instanceof Error ? error.message : "unknown error"
              }`
            );
          }
        })
      );
      const catchUpFailures = catchUpResults.flatMap((result) => {
        if (result.status === "fulfilled") {
          return [];
        }

        return [
          {
            error:
              result.reason instanceof Error
                ? result.reason.message
                : "Realtime restore catch-up failed",
            index: -1,
          },
        ];
      });
      failed.push(...catchUpFailures);
      reconciled = catchUpFailures.length === 0;
      if (reconciled) {
        this.updateLatestDeliveredOutboxSequence(socket, afterSequence);
      }
    } catch (error) {
      failed.push({
        error:
          error instanceof Error
            ? error.message
            : "Realtime restore catch-up failed",
        index: -1,
      });
    }

    // Registration failures (index >= 0) are rejections; the catch-up failure
    // (index -1) is a reconciliation error, not a rejected registration.
    const rejectedCount = failed.filter((entry) => entry.index >= 0).length;
    emitRealtimeMetric(
      REALTIME_RESTORE_SUBSCRIPTIONS_METRIC,
      subscriptions.length - rejectedCount,
      { result: "accepted" }
    );
    if (rejectedCount > 0) {
      emitRealtimeMetric(REALTIME_RESTORE_SUBSCRIPTIONS_METRIC, rejectedCount, {
        result: "rejected",
      });
    }
    await this.scheduleReconciliation();
    socket.send(JSON.stringify({ failed, reconciled, type: "restored" }));
  }

  private createRegistration(
    message: Record<string, unknown>,
    socket: RuntimeWebSocket
  ): RealtimeRegistration {
    const subscriptionId = getStringField(message, "subscriptionId");
    const attachment = this.socketRegistry.ensureAttachment(socket);
    const connectionKey = attachment.connectionKey;
    const args = message.args ?? {};
    assertJsonBounds(args, "Realtime subscription args");
    const queryName = getStringField(message, "queryName");
    const runtime = configuredRealtimeRuntimes.get(attachment.runtimeId);
    if (!runtime?.functionIndex.getByName("query", queryName, "public")) {
      throw new ValidationRuntimeError(
        `Realtime query "${queryName}" was not found`
      );
    }
    return {
      args,
      authorizationFingerprint: attachment.authorizationFingerprint,
      connectionKey,
      connectionName: attachment.connectionName,
      epoch: getEpoch(message.epoch),
      leaseExpiresAt: Date.now() + REALTIME_LEASE_MS,
      queryName,
      runtimeId: attachment.runtimeId,
      subscriptionId,
    };
  }

  private deliver(message: Record<string, unknown>): RealtimeDeliveryResult {
    const connectionKey = getStringField(message, "connectionKey");
    const shardName = getStringField(message, "shardName");
    const deliveries = message.deliveries;
    if (!Array.isArray(deliveries)) {
      throw new ValidationRuntimeError(
        'Realtime field "deliveries" must be an array'
      );
    }

    return this.socketRegistry.deliver(
      connectionKey,
      shardName,
      deliveries.map((delivery) => parseObject(delivery, "Realtime delivery"))
    );
  }

  private restoreHibernatedSockets(): void {
    const sockets = this.state.getWebSockets?.() ?? [];
    for (const socket of sockets) {
      const attachment = parseRealtimeSocketAttachment(
        socket.deserializeAttachment?.()
      );
      if (!attachment) {
        this.socketRegistry.closeExpiredSession(socket);
        continue;
      }

      this.socketRegistry.add(socket, attachment);
    }

    if (sockets.length > 0) {
      this.restoreAttachedSubscriptions({ reconcileAfterRestore: true }).catch(
        async (error: unknown) => {
          logRuntimeEvent(
            "error",
            "runtime.realtime_hibernation_restore_failed",
            {
              errorName: error instanceof Error ? error.name : typeof error,
            }
          );
          this.hasPendingHibernationRestoreRetry = true;
          try {
            await this.scheduleReconciliation();
          } catch (scheduleError) {
            logRuntimeEvent("error", "runtime.realtime_reconciliation_failed", {
              errorName:
                scheduleError instanceof Error
                  ? scheduleError.name
                  : typeof scheduleError,
            });
            emitRealtimeMetric(REALTIME_RECONCILIATIONS_METRIC, 1, {
              result: "failed",
            });
          }
        }
      );
    }
  }

  private async restoreAttachedSubscriptions(options: {
    readonly reconcileAfterRestore: boolean;
  }): Promise<{
    readonly accepted: number;
    readonly rejected: number;
  }> {
    const subscriptions = this.getAttachedSubscriptions();
    if (subscriptions.length === 0) {
      await this.scheduleReconciliation();
      return { accepted: 0, rejected: 0 };
    }

    const subscriptionsToRestore = subscriptions.filter(
      ({ attachment, subscription }) =>
        !this.restoredAttachedSubscriptionKeys.has(
          this.restoreKey(attachment, subscription)
        )
    );
    if (subscriptionsToRestore.length === 0) {
      this.hasPendingHibernationRestoreRetry = false;
      if (options.reconcileAfterRestore) {
        await this.catchUpActiveSubscriptions();
      }
      return { accepted: 0, rejected: 0 };
    }

    const subscriptionGeneration = await fetchActiveRealtimeShardGeneration(
      this.env.APP_DB
    );
    const staleRestoreAfterSequence = this.detectStaleGenerations(
      subscriptionsToRestore,
      subscriptionGeneration
    );
    const results = await Promise.allSettled(
      subscriptionsToRestore.map(({ attachment, subscription }) =>
        this.restoreAttachedSubscription(
          attachment,
          subscription,
          subscriptionGeneration
        )
      )
    );
    const rejected = this.logRejectedAttachedSubscriptionRestores(
      subscriptionsToRestore,
      results
    );
    if (rejected > 0) {
      emitRealtimeMetric(REALTIME_RESTORE_SUBSCRIPTIONS_METRIC, rejected, {
        result: "rejected",
      });
    }
    const accepted = subscriptionsToRestore.length - rejected;
    if (accepted > 0) {
      emitRealtimeMetric(REALTIME_RESTORE_SUBSCRIPTIONS_METRIC, accepted, {
        result: "accepted",
      });
    }
    this.hasPendingHibernationRestoreRetry = rejected > 0;
    let catchUpFailed = false;
    if (options.reconcileAfterRestore && accepted > 0) {
      const summary = await this.catchUpActiveSubscriptions(
        subscriptionGeneration,
        {
          activeGenerationFallbackAfterSequence:
            staleRestoreAfterSequence.afterSequence,
          includeActiveGenerationFallback:
            staleRestoreAfterSequence.hasStaleSubscriptions,
        }
      );
      catchUpFailed = summary.failed.length > 0;
      this.emitReconciliationSummary(summary);
    }
    if (rejected > 0 || catchUpFailed) {
      await this.scheduleReconciliation();
    }
    return { accepted, rejected };
  }

  private logRejectedAttachedSubscriptionRestores(
    subscriptions: readonly {
      readonly attachment: RealtimeSocketAttachment;
      readonly subscription: RealtimeSocketSubscription;
    }[],
    results: readonly PromiseSettledResult<void>[]
  ): number {
    let rejected = 0;
    for (const [index, result] of results.entries()) {
      if (result.status !== "rejected") {
        continue;
      }
      rejected += 1;
      logRuntimeEvent(
        "error",
        "runtime.realtime_hibernation_subscription_restore_failed",
        {
          errorMessage:
            result.reason instanceof Error ? result.reason.message : undefined,
          errorName:
            result.reason instanceof Error
              ? result.reason.name
              : typeof result.reason,
          queryName: subscriptions[index]?.subscription.queryName,
          subscriptionShardName:
            subscriptions[index]?.subscription.subscriptionShardName,
          subscriptionId: subscriptions[index]?.subscription.subscriptionId,
        }
      );
    }

    return rejected;
  }

  private async restoreAttachedSubscription(
    attachment: RealtimeSocketAttachment,
    subscription: RealtimeSocketSubscription,
    subscriptionGeneration: RealtimeShardGeneration
  ): Promise<void> {
    const subscriptionShardName = this.currentGenerationShardName(
      subscription.subscriptionShardName,
      subscriptionGeneration
    );
    const subscriptionTarget = await this.subscriptionTarget(
      subscriptionShardName,
      createRealtimeGlobalSubscriptionRouteTarget(),
      subscriptionGeneration
    );
    const response = await subscriptionTarget.stub.fetch(
      "https://baseflare.internal/register",
      {
        body: JSON.stringify({
          args: subscription.args,
          authorizationFingerprint: attachment.authorizationFingerprint,
          connectionKey: attachment.connectionKey,
          connectionName: attachment.connectionName,
          epoch: subscription.epoch,
          leaseExpiresAt: Date.now() + REALTIME_LEASE_MS,
          queryName: subscription.queryName,
          runtimeId: attachment.runtimeId,
          shardName: subscriptionTarget.shardName,
          subscriptionId: subscription.subscriptionId,
        }),
        headers: JSON_HEADERS,
        method: "POST",
      }
    );
    if (!response.ok) {
      throw new InternalRuntimeError(
        `Realtime subscription registration failed with status ${response.status}`
      );
    }
    this.socketRegistry.updateSubscriptionShardName(
      attachment.connectionKey,
      subscription.subscriptionId,
      subscriptionTarget.shardName
    );
    this.restoredAttachedSubscriptionKeys.add(
      this.restoreKey(attachment, {
        ...subscription,
        subscriptionShardName: subscriptionTarget.shardName,
      })
    );
  }

  private currentGenerationShardName(
    shardName: string | undefined,
    generation: RealtimeShardGeneration
  ): string | undefined {
    if (
      shardName &&
      parseRealtimeSubscriptionShardName(shardName)?.generationId ===
        generation.generationId
    ) {
      return shardName;
    }

    return undefined;
  }

  private getAttachedSubscriptions(): Array<{
    readonly attachment: RealtimeSocketAttachment;
    readonly subscription: RealtimeSocketSubscription;
  }> {
    return this.socketRegistry.attachedSubscriptions();
  }

  private updateLatestDeliveredOutboxSequence(
    socket: RuntimeWebSocket,
    sequence: number | null
  ): void {
    this.socketRegistry.updateDeliveredSequence(socket, sequence);
  }

  private updateSubscriptionShardName(message: Record<string, unknown>): void {
    const connectionKey = getStringField(message, "connectionKey");
    const subscriptionId = getStringField(message, "subscriptionId");
    const subscriptionShardName = getStringField(
      message,
      "subscriptionShardName"
    );
    const updates = this.socketRegistry.updateSubscriptionShardName(
      connectionKey,
      subscriptionId,
      subscriptionShardName
    );
    for (const update of updates) {
      const wasRestored = this.restoredAttachedSubscriptionKeys.delete(
        this.restoreKey(update.previousAttachment, update.previousSubscription)
      );
      if (wasRestored) {
        this.restoredAttachedSubscriptionKeys.add(
          this.restoreKey(update.nextAttachment, update.nextSubscription)
        );
      }
    }
  }

  private restoreKey(
    attachment: RealtimeSocketAttachment,
    subscription: RealtimeSocketSubscription
  ): string {
    return JSON.stringify([
      attachment.connectionKey,
      subscription.subscriptionId,
      subscription.epoch,
      subscription.subscriptionShardName ?? "",
    ]);
  }

  private forgetRestored(
    attachment: RealtimeSocketAttachment,
    subscriptionId?: string
  ): void {
    if (!Array.isArray(attachment.subscriptions)) {
      return;
    }

    for (const subscription of attachment.subscriptions) {
      if (subscriptionId && subscription.subscriptionId !== subscriptionId) {
        continue;
      }
      this.restoredAttachedSubscriptionKeys.delete(
        this.restoreKey(attachment, subscription)
      );
    }
  }

  private sendSocketError(socket: RuntimeWebSocket, error: unknown): void {
    try {
      socket.send(
        JSON.stringify({
          error:
            error instanceof Error ? error.message : "Realtime message failed",
          type: "error",
        })
      );
    } catch {
      // Best-effort error reporting: the socket may already be closing.
    }
  }

  private sendSubscriptionAcknowledgement(
    socket: RuntimeWebSocket,
    subscriptionId: string
  ): void {
    socket.send(
      JSON.stringify({
        subscriptionId,
        type: "subscribed",
      })
    );
  }

  private hasActiveSocketSubscriptions(): boolean {
    return this.socketRegistry.hasActiveSubscriptions();
  }

  private async reconcileActiveSubscriptions(): Promise<void> {
    if (!this.hasActiveSocketSubscriptions()) {
      emitRealtimeMetric(REALTIME_RECONCILIATIONS_METRIC, 1, {
        result: "skipped",
      });
      await this.clearReconciliationAlarm();
      return;
    }

    try {
      if (this.hasPendingHibernationRestoreRetry) {
        const restoreResult = await this.restoreAttachedSubscriptions({
          reconcileAfterRestore: false,
        });
        if (restoreResult.rejected > 0) {
          if (restoreResult.accepted > 0) {
            const summary = await this.catchUpActiveSubscriptions();
            this.emitReconciliationSummary(summary);
          }
          await this.scheduleReconciliation();
          return;
        }
      }
      const summary = await this.catchUpActiveSubscriptions();
      this.emitReconciliationSummary(summary);
    } catch (error) {
      logRuntimeEvent("error", "runtime.realtime_reconciliation_failed", {
        errorName: error instanceof Error ? error.name : typeof error,
      });
      emitRealtimeMetric(REALTIME_RECONCILIATIONS_METRIC, 1, {
        result: "failed",
      });
    }

    await this.scheduleReconciliation();
  }

  private async catchUpActiveSubscriptions(
    generationOverride?: RealtimeShardGeneration,
    options: {
      readonly activeGenerationFallbackAfterSequence?: number | null;
      readonly includeActiveGenerationFallback?: boolean;
    } = {}
  ): Promise<RealtimeReconciliationSummary> {
    const targets = await this.reconciliationCatchUpTargets(
      generationOverride,
      options
    );
    const reconciliationSession = this.env.APP_DB.withSession?.(
      "first-unconstrained"
    );
    const outboxBookmark = reconciliationSession?.getBookmark();
    const results = await Promise.allSettled(
      targets.map(async (target) => {
        try {
          const response = await target.stub.fetch(
            "https://baseflare.internal/catch-up",
            {
              body: JSON.stringify({
                afterSequence: target.afterSequence,
                outboxBookmark,
                shardName: target.shardName,
              }),
              headers: JSON_HEADERS,
              method: "POST",
            }
          );
          if (!response.ok) {
            return {
              failed: {
                errorName: "InternalRuntimeError",
                message: `Realtime reconciliation failed for shard ${target.shardName} with status ${response.status}`,
                shardName: target.shardName,
                status: response.status,
              },
            };
          }
          return {
            latestSequence: await this.readCatchUpLatestSequence(response),
            shardName: target.shardName,
          };
        } catch (error) {
          return {
            failed: {
              errorName: error instanceof Error ? error.name : typeof error,
              message: `Realtime reconciliation failed for shard ${
                target.shardName
              }: ${error instanceof Error ? error.message : "unknown error"}`,
              shardName: target.shardName,
            },
          };
        }
      })
    );
    const failed: RealtimeReconciliationFailure[] = [];
    const latestSequencesByShard = new Map<string, number>();
    for (const [index, result] of results.entries()) {
      const target = targets.at(index);
      if (!target) {
        continue;
      }

      if (result.status === "rejected") {
        failed.push({
          errorName:
            result.reason instanceof Error ? result.reason.name : "unknown",
          message: `Realtime reconciliation failed for shard ${
            target.shardName
          }: ${
            result.reason instanceof Error
              ? result.reason.message
              : "unknown error"
          }`,
          shardName: target.shardName,
        });
        continue;
      }

      if ("failed" in result.value) {
        const failure = result.value.failed;
        if (failure) {
          failed.push(failure);
        }
        continue;
      }

      if (result.value.latestSequence !== null) {
        latestSequencesByShard.set(
          result.value.shardName,
          result.value.latestSequence
        );
      }
    }

    return {
      attempted: targets.length,
      failed,
      latestSequencesByShard,
      succeeded: targets.length - failed.length,
    };
  }

  private async readCatchUpLatestSequence(
    response: Response
  ): Promise<number | null> {
    try {
      const body = (await response.json()) as {
        readonly events?: unknown;
        readonly latestSequence?: unknown;
      };
      if (Number.isSafeInteger(body.latestSequence)) {
        return body.latestSequence as number;
      }
      if (!Array.isArray(body.events)) {
        return null;
      }

      let latestSequence: number | null = null;
      for (const event of body.events) {
        if (
          typeof event === "object" &&
          event !== null &&
          "sequence" in event &&
          Number.isSafeInteger(event.sequence)
        ) {
          latestSequence =
            latestSequence === null
              ? (event.sequence as number)
              : Math.max(latestSequence, event.sequence as number);
        }
      }
      return latestSequence;
    } catch {
      return null;
    }
  }

  private emitReconciliationSummary(
    summary: RealtimeReconciliationSummary
  ): void {
    this.socketRegistry.updateDeliveredSequencesForReconciledShards(
      summary.latestSequencesByShard,
      new Set(summary.failed.map((failure) => failure.shardName))
    );

    if (summary.failed.length > 0) {
      logRuntimeEvent("error", "runtime.realtime_reconciliation_failed", {
        attempted: summary.attempted,
        failedCount: summary.failed.length,
        failedShards: summary.failed.map((failure) => failure.shardName),
        statuses: summary.failed
          .map((failure) => failure.status)
          .filter((status): status is number => typeof status === "number"),
        succeeded: summary.succeeded,
      });
    }

    const metricResult = this.reconciliationMetricResult(summary);
    emitRealtimeMetric(REALTIME_RECONCILIATIONS_METRIC, 1, {
      result: metricResult,
    });
  }

  private reconciliationMetricResult(
    summary: RealtimeReconciliationSummary
  ): "failed" | "partial" | "reconciled" {
    if (summary.failed.length === 0) {
      return "reconciled";
    }

    return summary.succeeded > 0 ? "partial" : "failed";
  }

  private async reconciliationCatchUpTargets(
    generationOverride?: RealtimeShardGeneration,
    options: {
      readonly activeGenerationFallbackAfterSequence?: number | null;
      readonly includeActiveGenerationFallback?: boolean;
    } = {}
  ): Promise<
    Array<{
      readonly afterSequence: number | null;
      readonly shardName: string;
      readonly stub: DurableObjectStub;
    }>
  > {
    const targets = new Map<
      string,
      {
        afterSequence: number | null;
        shardName: string;
        stub: DurableObjectStub;
      }
    >();
    let defaultTarget:
      | {
          readonly shardName: string;
          readonly stub: DurableObjectStub;
        }
      | undefined;

    for (const {
      attachment,
      subscription,
    } of this.socketRegistry.attachedSubscriptions()) {
      let target: {
        readonly shardName: string;
        readonly stub: DurableObjectStub;
      };
      if (subscription.subscriptionShardName == null) {
        defaultTarget ??= await this.subscriptionTarget();
        target = defaultTarget;
      } else {
        target = {
          shardName: subscription.subscriptionShardName,
          stub: this.env.REALTIME_SUBSCRIPTIONS.get(
            this.env.REALTIME_SUBSCRIPTIONS.idFromName(
              subscription.subscriptionShardName
            )
          ),
        };
      }

      const existing = targets.get(target.shardName);
      const afterSequence = this.minAfterSequence(
        existing?.afterSequence ?? null,
        attachment.latestDeliveredOutboxSequence
      );
      targets.set(target.shardName, {
        afterSequence,
        shardName: target.shardName,
        stub: target.stub,
      });
    }

    if (targets.size === 0) {
      const target = await this.subscriptionTarget();
      return [
        {
          afterSequence: null,
          shardName: target.shardName,
          stub: target.stub,
        },
      ];
    }

    const activeGeneration =
      generationOverride ??
      (await fetchActiveRealtimeShardGeneration(this.env.APP_DB));
    const staleAfterSequence = this.detectStaleGenerations(
      this.socketRegistry.attachedSubscriptions(),
      activeGeneration
    );
    if (
      staleAfterSequence.hasStaleSubscriptions ||
      options.includeActiveGenerationFallback
    ) {
      const fallbackAfterSequence =
        options.activeGenerationFallbackAfterSequence ??
        staleAfterSequence.afterSequence;
      for (const shardName of getRealtimeSubscriptionShardNames(
        activeGeneration
      )) {
        if (targets.has(shardName)) {
          continue;
        }

        targets.set(shardName, {
          afterSequence: fallbackAfterSequence,
          shardName,
          stub: this.env.REALTIME_SUBSCRIPTIONS.get(
            this.env.REALTIME_SUBSCRIPTIONS.idFromName(shardName)
          ),
        });
      }
    }

    return [...targets.values()];
  }

  private detectStaleGenerations(
    subscriptions: readonly {
      readonly attachment: RealtimeSocketAttachment;
      readonly subscription: RealtimeSocketSubscription;
    }[],
    activeGeneration: RealtimeShardGeneration
  ): {
    readonly afterSequence: number | null;
    readonly hasStaleSubscriptions: boolean;
  } {
    let afterSequence: number | null = null;
    let hasStaleSubscriptions = false;
    for (const { attachment, subscription } of subscriptions) {
      const shardName = subscription.subscriptionShardName;
      const parsedShardName = shardName
        ? parseRealtimeSubscriptionShardName(shardName)
        : undefined;
      if (
        !shardName ||
        parsedShardName?.generationId === activeGeneration.generationId
      ) {
        continue;
      }

      hasStaleSubscriptions = true;
      afterSequence = this.minAfterSequence(
        afterSequence,
        attachment.latestDeliveredOutboxSequence
      );
    }

    return { afterSequence, hasStaleSubscriptions };
  }

  private minAfterSequence(
    current: number | null,
    candidate: number | null
  ): number | null {
    if (candidate == null) {
      return current;
    }

    return current == null ? candidate : Math.min(current, candidate);
  }

  private async scheduleReconciliation(): Promise<void> {
    try {
      if (!this.hasActiveSocketSubscriptions()) {
        await this.clearReconciliationAlarm();
        return;
      }

      const pendingAlarm = await this.state.storage?.getAlarm();
      if (pendingAlarm != null) {
        return;
      }

      await this.state.storage?.setAlarm(
        Date.now() + REALTIME_RECONCILIATION_INTERVAL_MS
      );
    } catch (error) {
      // The reconciliation alarm is an opportunistic safety net that is
      // re-attempted on every register/deliver/restore/alarm pass, so a
      // transient storage failure here must never fail an operation that
      // already succeeded (e.g. swallow a client acknowledgement).
      logRuntimeEvent(
        "warn",
        "runtime.realtime_reconciliation_schedule_failed",
        {
          errorName: error instanceof Error ? error.name : typeof error,
        }
      );
    }
  }

  private async clearReconciliationAlarm(): Promise<void> {
    await this.state.storage?.deleteAlarm();
  }

  private async subscriptionTarget(
    shardName?: string,
    route: RealtimeSubscriptionRouteTarget = createRealtimeGlobalSubscriptionRouteTarget(),
    generationOverride?: RealtimeShardGeneration
  ): Promise<{
    readonly generation: RealtimeShardGeneration;
    readonly shardName: string;
    readonly stub: DurableObjectStub;
  }> {
    const generation =
      generationOverride ??
      (await fetchActiveRealtimeShardGeneration(this.env.APP_DB));
    const resolvedShardName =
      shardName ?? getRealtimeSubscriptionShardName(route, generation);
    return {
      generation,
      shardName: resolvedShardName,
      stub: this.env.REALTIME_SUBSCRIPTIONS.get(
        this.env.REALTIME_SUBSCRIPTIONS.idFromName(resolvedShardName)
      ),
    };
  }

  private async subscriptionCatchUpTargets(
    socket?: RuntimeWebSocket,
    generationOverride?: RealtimeShardGeneration
  ): Promise<
    Array<{
      readonly shardName: string;
      readonly stub: DurableObjectStub;
    }>
  > {
    const shardNames = new Set<string>();
    const activeGeneration =
      generationOverride ??
      (await fetchActiveRealtimeShardGeneration(this.env.APP_DB));
    const subscriptions = socket
      ? (this.socketRegistry.getAttachment(socket)?.subscriptions ?? [])
      : this.socketRegistry
          .attachedSubscriptions()
          .map(({ subscription }) => subscription);
    for (const subscription of subscriptions) {
      if (subscription.subscriptionShardName) {
        shardNames.add(subscription.subscriptionShardName);
      }
    }
    const hasStaleGenerationSubscription = subscriptions.some(
      (subscription) =>
        subscription.subscriptionShardName &&
        getRealtimeShardGenerationIdFromName(
          subscription.subscriptionShardName
        ) !== activeGeneration.generationId
    );
    if (hasStaleGenerationSubscription) {
      for (const shardName of getRealtimeSubscriptionShardNames(
        activeGeneration
      )) {
        shardNames.add(shardName);
      }
    }

    return [...shardNames].map((shardName) => ({
      shardName,
      stub: this.env.REALTIME_SUBSCRIPTIONS.get(
        this.env.REALTIME_SUBSCRIPTIONS.idFromName(shardName)
      ),
    }));
  }
}
