import { InternalRuntimeError, ValidationRuntimeError } from "../errors";
import { logRuntimeEvent } from "../logging";
import type { BaseflareRuntimeEnv, DurableObjectStub } from "../types";
import {
  createRealtimeGlobalSubscriptionRouteTarget,
  getRealtimeConnectionShardName,
  getRealtimeSubscriptionShardName,
} from "./routing";
import { fetchActiveRealtimeShardGeneration } from "./shards";
import {
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
import type {
  RealtimeDeliveryResult,
  RealtimeDurableObjectState,
  RealtimeObjectEnv,
  RealtimeRegistration,
  RealtimeShardGeneration,
  RealtimeSocketAttachment,
  RealtimeSocketState,
  RealtimeSocketSubscription,
  RealtimeSubscriptionRouteTarget,
  RuntimeWebSocket,
} from "./types";
import {
  JSON_HEADERS,
  REALTIME_CONNECTION_KEY_HEADER,
  REALTIME_LEASE_MS,
  REALTIME_MAX_RESTORE_SUBSCRIPTIONS,
  REALTIME_RECONCILIATION_INTERVAL_MS,
  REALTIME_RECONCILIATIONS_METRIC,
  REALTIME_RESTORE_SUBSCRIPTIONS_METRIC,
  REALTIME_RUNTIME_ID_HEADER,
} from "./types";

declare const WebSocketPair: {
  new (): { readonly 0: WebSocket; readonly 1: RuntimeWebSocket };
};

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

  const clientKey = resolveRealtimeConnectionKey(url);
  const shardName = getRealtimeConnectionShardName(clientKey);
  const stub = env.REALTIME_CONNECTIONS.get(
    env.REALTIME_CONNECTIONS.idFromName(shardName)
  );
  const headers = new Headers(request.headers);
  headers.set(REALTIME_CONNECTION_KEY_HEADER, clientKey);
  headers.set(REALTIME_RUNTIME_ID_HEADER, runtimeId);

  return await stub.fetch(new Request(request, { headers }));
}

export class RealtimeConnectionDO {
  private readonly env: RealtimeObjectEnv;
  private readonly socketStates = new Map<
    RuntimeWebSocket,
    RealtimeSocketState
  >();
  private readonly socketsByConnectionKey = new Map<
    string,
    Set<RuntimeWebSocket>
  >();
  private readonly sockets = new Set<RuntimeWebSocket>();
  private readonly state: RealtimeDurableObjectState;
  private hasPendingHibernationRestoreRetry = false;

  constructor(
    state: RealtimeDurableObjectState | unknown,
    env: RealtimeObjectEnv
  ) {
    this.state = isRealtimeDurableObjectState(state) ? state : {};
    this.env = env;
    this.restoreHibernatedSockets();
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "GET") {
      return this.acceptWebSocket(request);
    }

    if (request.method === "POST" && url.pathname === "/deliver") {
      const message = await readJsonObject(request);
      const delivered = this.deliver(message);
      return jsonResponse({ ...delivered, ok: true });
    }

    if (request.method === "POST" && url.pathname === "/subscription-moved") {
      const message = await readJsonObject(request);
      this.updateSubscriptionShardName(message);
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
      request.headers.get(REALTIME_CONNECTION_KEY_HEADER) ??
      resolveRealtimeConnectionKey(url);
    const authorizationHeader = request.headers.get("authorization");
    const runtimeId = request.headers.get(REALTIME_RUNTIME_ID_HEADER) ?? "";
    const attachment = createRealtimeSocketAttachment({
      authorizationHeader,
      connectionKey: clientKey,
      runtimeId,
    });
    if (this.state.acceptWebSocket) {
      this.state.acceptWebSocket(server);
      this.addSocket(server, attachment);
    } else {
      server.accept();
      this.addSocket(server, attachment);
      server.addEventListener("message", (event: MessageEvent) => {
        this.handleMessage(server, event.data).catch((error: unknown) => {
          this.sendSocketError(server, error);
        });
      });
      server.addEventListener("close", () => {
        this.removeSocket(server);
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
    this.removeSocket(socket);
  }

  webSocketError(socket: RuntimeWebSocket): void {
    this.removeSocket(socket);
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
    socket: RuntimeWebSocket,
    options: {
      readonly scheduleReconciliation?: boolean;
      readonly sendAcknowledgement?: boolean;
      readonly subscriptionGeneration?: RealtimeShardGeneration;
    } = {}
  ): Promise<void> {
    const registration = this.createRegistration(message, socket);
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

    this.addSocketSubscription(socket, {
      args: message.args ?? {},
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
      this.getSocketAttachment(socket)?.connectionKey ?? "default";
    const existingSubscription = this.getSocketSubscription(
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

    this.removeSocketSubscription(socket, subscriptionId);
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
      const catchUpTargets = await this.subscriptionCatchUpTargets(socket);
      await Promise.all(
        catchUpTargets.map(async (catchUpTarget) => {
          const catchUpResponse = await catchUpTarget.stub.fetch(
            "https://baseflare.internal/catch-up",
            {
              body: JSON.stringify({
                afterSequence,
                shardName: catchUpTarget.shardName,
              }),
              headers: JSON_HEADERS,
              method: "POST",
            }
          );
          if (!catchUpResponse.ok) {
            throw new InternalRuntimeError(
              `Realtime restore catch-up failed with status ${catchUpResponse.status}`
            );
          }
        })
      );
      reconciled = true;
      this.updateLatestDeliveredOutboxSequence(socket, afterSequence);
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
    socket?: RuntimeWebSocket
  ): RealtimeRegistration {
    const subscriptionId = getStringField(message, "subscriptionId");
    const attachment = socket ? this.getSocketAttachment(socket) : undefined;
    const connectionKey = attachment?.connectionKey ?? "default";
    return {
      args: message.args ?? {},
      authorizationHeader: attachment?.authorizationHeader,
      connectionKey,
      connectionName:
        attachment?.connectionName ??
        getRealtimeConnectionShardName(connectionKey),
      epoch: getEpoch(message.epoch),
      leaseExpiresAt: Date.now() + REALTIME_LEASE_MS,
      queryName: getStringField(message, "queryName"),
      runtimeId: attachment?.runtimeId ?? "",
      subscriptionId,
    };
  }

  private addSocket(
    socket: RuntimeWebSocket,
    attachment: RealtimeSocketAttachment
  ): void {
    this.sockets.add(socket);
    this.setSocketAttachment(socket, attachment);
    const sockets =
      this.socketsByConnectionKey.get(attachment.connectionKey) ??
      new Set<RuntimeWebSocket>();
    sockets.add(socket);
    this.socketsByConnectionKey.set(attachment.connectionKey, sockets);
  }

  private removeSocket(socket: RuntimeWebSocket): void {
    this.sockets.delete(socket);
    const connectionKey = this.getSocketAttachment(socket)?.connectionKey;
    this.socketStates.delete(socket);
    if (!connectionKey) {
      return;
    }

    const sockets = this.socketsByConnectionKey.get(connectionKey);
    sockets?.delete(socket);
    if (sockets?.size === 0) {
      this.socketsByConnectionKey.delete(connectionKey);
    }
  }

  private deliver(message: Record<string, unknown>): RealtimeDeliveryResult {
    const connectionKey = getStringField(message, "connectionKey");
    const deliveries = message.deliveries;
    if (!Array.isArray(deliveries)) {
      throw new ValidationRuntimeError(
        'Realtime field "deliveries" must be an array'
      );
    }

    return this.deliverMessages(
      connectionKey,
      deliveries.map((delivery) => parseObject(delivery, "Realtime delivery"))
    );
  }

  private deliverMessages(
    connectionKey: string,
    messages: readonly Record<string, unknown>[]
  ): RealtimeDeliveryResult {
    const sockets = this.socketsByConnectionKey.get(connectionKey);
    if (!sockets || messages.length === 0) {
      return { delivered: 0, deliveredSubscriptions: [] };
    }

    let delivered = 0;
    const deliveredSubscriptions = new Set<string>();
    for (const message of messages) {
      const subscriptionId = getStringField(message, "subscriptionId");
      const sent = this.deliverMessageToSockets(sockets, message);
      delivered += sent;
      if (sent > 0) {
        deliveredSubscriptions.add(subscriptionId);
      }
    }

    return {
      delivered,
      deliveredSubscriptions: [...deliveredSubscriptions],
    };
  }

  private deliverMessageToSockets(
    sockets: Set<RuntimeWebSocket>,
    message: Record<string, unknown>
  ): number {
    const payload = JSON.stringify({
      message,
      type: "delivery",
    });
    let delivered = 0;
    for (const socket of [...sockets]) {
      try {
        socket.send(payload);
        this.updateSocketDeliveredOutboxSequence(socket, message.sequence);
        delivered += 1;
      } catch {
        this.removeSocket(socket);
      }
    }

    return delivered;
  }

  private restoreHibernatedSockets(): void {
    const sockets = this.state.getWebSockets?.() ?? [];
    for (const socket of sockets) {
      const attachment = parseRealtimeSocketAttachment(
        socket.deserializeAttachment?.()
      );
      if (!attachment) {
        continue;
      }

      this.addSocket(socket, attachment);
    }

    if (sockets.length > 0) {
      this.restoreAttachedSubscriptions({ reconcileAfterRestore: true }).catch(
        (error: unknown) => {
          logRuntimeEvent(
            "error",
            "runtime.realtime_hibernation_restore_failed",
            {
              errorName: error instanceof Error ? error.name : typeof error,
            }
          );
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

    const results = await Promise.allSettled(
      subscriptions.map(async ({ attachment, subscription }) => {
        const subscriptionTarget = await this.subscriptionTarget(
          subscription.subscriptionShardName
        );
        const response = await subscriptionTarget.stub.fetch(
          "https://baseflare.internal/register",
          {
            body: JSON.stringify({
              args: subscription.args,
              authorizationHeader: attachment.authorizationHeader,
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
      })
    );
    // Surface per-subscription registration failures (e.g. a subscription DO
    // shard transiently unavailable on hibernation wakeup) instead of silently
    // swallowing them — otherwise the subscription is stranded with no signal.
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
    if (rejected > 0) {
      emitRealtimeMetric(REALTIME_RESTORE_SUBSCRIPTIONS_METRIC, rejected, {
        result: "rejected",
      });
    }
    const accepted = subscriptions.length - rejected;
    if (accepted > 0) {
      emitRealtimeMetric(REALTIME_RESTORE_SUBSCRIPTIONS_METRIC, accepted, {
        result: "accepted",
      });
    }
    this.hasPendingHibernationRestoreRetry = rejected > 0;
    let catchUpFailed = false;
    if (options.reconcileAfterRestore && accepted > 0) {
      try {
        await this.catchUpActiveSubscriptions();
      } catch (error) {
        catchUpFailed = true;
        logRuntimeEvent("error", "runtime.realtime_reconciliation_failed", {
          errorName: error instanceof Error ? error.name : typeof error,
        });
        emitRealtimeMetric(REALTIME_RECONCILIATIONS_METRIC, 1, {
          result: "failed",
        });
      }
    }
    if (rejected > 0 || catchUpFailed) {
      await this.scheduleReconciliation();
    }
    return { accepted, rejected };
  }

  private getAttachedSubscriptions(): Array<{
    readonly attachment: RealtimeSocketAttachment;
    readonly subscription: RealtimeSocketSubscription;
  }> {
    const attachedSubscriptions: Array<{
      readonly attachment: RealtimeSocketAttachment;
      readonly subscription: RealtimeSocketSubscription;
    }> = [];
    for (const { attachment } of this.socketStates.values()) {
      for (const subscription of attachment.subscriptions) {
        attachedSubscriptions.push({ attachment, subscription });
      }
    }

    return attachedSubscriptions;
  }

  private addSocketSubscription(
    socket: RuntimeWebSocket,
    subscription: RealtimeSocketSubscription
  ): void {
    const attachment = this.getSocketAttachment(socket);
    if (!attachment) {
      return;
    }

    const subscriptions = attachment.subscriptions.filter(
      (existing) => existing.subscriptionId !== subscription.subscriptionId
    );
    this.setSocketAttachment(socket, {
      ...attachment,
      subscriptions: [...subscriptions, subscription],
    });
  }

  private removeSocketSubscription(
    socket: RuntimeWebSocket,
    subscriptionId: string
  ): void {
    const attachment = this.getSocketAttachment(socket);
    if (!attachment) {
      return;
    }

    this.setSocketAttachment(socket, {
      ...attachment,
      subscriptions: attachment.subscriptions.filter(
        (subscription) => subscription.subscriptionId !== subscriptionId
      ),
    });
  }

  private getSocketSubscription(
    socket: RuntimeWebSocket,
    subscriptionId: string
  ): RealtimeSocketSubscription | undefined {
    return this.getSocketAttachment(socket)?.subscriptions.find(
      (subscription) => subscription.subscriptionId === subscriptionId
    );
  }

  private updateSocketDeliveredOutboxSequence(
    socket: RuntimeWebSocket,
    sequence: unknown
  ): void {
    if (sequence == null || !Number.isSafeInteger(sequence)) {
      return;
    }

    const attachment = this.getSocketAttachment(socket);
    if (!attachment) {
      return;
    }

    this.setSocketAttachment(socket, {
      ...attachment,
      latestDeliveredOutboxSequence: Math.max(
        attachment.latestDeliveredOutboxSequence ?? 0,
        sequence as number
      ),
    });
  }

  private updateLatestDeliveredOutboxSequence(
    socket: RuntimeWebSocket,
    sequence: number | null
  ): void {
    this.updateSocketDeliveredOutboxSequence(socket, sequence);
  }

  private updateSubscriptionShardName(message: Record<string, unknown>): void {
    const connectionKey = getStringField(message, "connectionKey");
    const subscriptionId = getStringField(message, "subscriptionId");
    const subscriptionShardName = getStringField(
      message,
      "subscriptionShardName"
    );
    const sockets = this.socketsByConnectionKey.get(connectionKey);
    if (!sockets) {
      return;
    }

    for (const socket of sockets) {
      const attachment = this.getSocketAttachment(socket);
      if (!attachment) {
        continue;
      }

      this.setSocketAttachment(socket, {
        ...attachment,
        subscriptions: attachment.subscriptions.map((subscription) =>
          subscription.subscriptionId === subscriptionId
            ? { ...subscription, subscriptionShardName }
            : subscription
        ),
      });
    }
  }

  private getSocketAttachment(
    socket: RuntimeWebSocket
  ): RealtimeSocketAttachment | undefined {
    return (
      this.socketStates.get(socket)?.attachment ??
      parseRealtimeSocketAttachment(socket.deserializeAttachment?.()) ??
      undefined
    );
  }

  private setSocketAttachment(
    socket: RuntimeWebSocket,
    attachment: RealtimeSocketAttachment
  ): void {
    this.socketStates.set(socket, { attachment });
    socket.serializeAttachment?.(attachment);
  }

  private sendSocketError(socket: RuntimeWebSocket, error: unknown): void {
    socket.send(
      JSON.stringify({
        error:
          error instanceof Error ? error.message : "Realtime message failed",
        type: "error",
      })
    );
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

  private getReconciliationAfterSequence(): number | null {
    let afterSequence: number | null = null;
    for (const { attachment } of this.socketStates.values()) {
      if (attachment.subscriptions.length === 0) {
        continue;
      }

      if (attachment.latestDeliveredOutboxSequence == null) {
        return null;
      }

      afterSequence =
        afterSequence == null
          ? attachment.latestDeliveredOutboxSequence
          : Math.min(afterSequence, attachment.latestDeliveredOutboxSequence);
    }

    return afterSequence;
  }

  private hasActiveSocketSubscriptions(): boolean {
    return [...this.socketStates.values()].some(
      ({ attachment }) => attachment.subscriptions.length > 0
    );
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
          return;
        }
      }
      await this.catchUpActiveSubscriptions();
      emitRealtimeMetric(REALTIME_RECONCILIATIONS_METRIC, 1, {
        result: "reconciled",
      });
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

  private async catchUpActiveSubscriptions(): Promise<void> {
    const afterSequence = this.getReconciliationAfterSequence();
    const targets = await this.subscriptionCatchUpTargets();
    await Promise.all(
      targets.map(async (target) => {
        const response = await target.stub.fetch(
          "https://baseflare.internal/catch-up",
          {
            body: JSON.stringify({
              afterSequence,
              shardName: target.shardName,
            }),
            headers: JSON_HEADERS,
            method: "POST",
          }
        );
        if (!response.ok) {
          throw new InternalRuntimeError(
            `Realtime reconciliation failed with status ${response.status}`
          );
        }
      })
    );
  }

  private async scheduleReconciliation(): Promise<void> {
    if (!this.hasActiveSocketSubscriptions()) {
      await this.clearReconciliationAlarm();
      return;
    }

    await this.state.storage?.setAlarm?.(
      Date.now() + REALTIME_RECONCILIATION_INTERVAL_MS
    );
  }

  private async clearReconciliationAlarm(): Promise<void> {
    await this.state.storage?.deleteAlarm?.();
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

  private async subscriptionCatchUpTargets(socket?: RuntimeWebSocket): Promise<
    Array<{
      readonly shardName: string;
      readonly stub: DurableObjectStub;
    }>
  > {
    const shardNames = new Set<string>();
    const attachments = socket
      ? [this.getSocketAttachment(socket)].filter(
          (attachment): attachment is RealtimeSocketAttachment =>
            attachment !== undefined
        )
      : [...this.socketStates.values()].map(({ attachment }) => attachment);
    for (const attachment of attachments) {
      for (const subscription of attachment.subscriptions) {
        if (subscription.subscriptionShardName) {
          shardNames.add(subscription.subscriptionShardName);
        }
      }
    }

    if (shardNames.size === 0) {
      const target = await this.subscriptionTarget();
      return [target];
    }

    return [...shardNames].map((shardName) => ({
      shardName,
      stub: this.env.REALTIME_SUBSCRIPTIONS.get(
        this.env.REALTIME_SUBSCRIPTIONS.idFromName(shardName)
      ),
    }));
  }
}
