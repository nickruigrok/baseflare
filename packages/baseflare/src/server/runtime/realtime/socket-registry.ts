import { InternalRuntimeError } from "../errors";
import { getStringField, parseRealtimeSocketAttachment } from "./shared";
import type {
  RealtimeDeliveryResult,
  RealtimeSocketAttachment,
  RealtimeSocketState,
  RealtimeSocketSubscription,
  RuntimeWebSocket,
} from "./types";

export interface RealtimeSocketRegistrySnapshot {
  readonly socketStates: Map<RuntimeWebSocket, RealtimeSocketState>;
  readonly sockets: Set<RuntimeWebSocket>;
  readonly socketsByConnectionKey: Map<string, Set<RuntimeWebSocket>>;
}

export interface RealtimeSocketSubscriptionUpdate {
  readonly nextAttachment: RealtimeSocketAttachment;
  readonly nextSubscription: RealtimeSocketSubscription;
  readonly previousAttachment: RealtimeSocketAttachment;
  readonly previousSubscription: RealtimeSocketSubscription;
}

export class RealtimeSocketRegistry {
  private readonly onRemoveAttachment?: (
    attachment: RealtimeSocketAttachment
  ) => void;
  private readonly socketStates = new Map<
    RuntimeWebSocket,
    RealtimeSocketState
  >();
  private readonly sockets = new Set<RuntimeWebSocket>();
  private readonly socketsByConnectionKey = new Map<
    string,
    Set<RuntimeWebSocket>
  >();

  constructor(options?: {
    readonly onRemoveAttachment?: (
      attachment: RealtimeSocketAttachment
    ) => void;
  }) {
    this.onRemoveAttachment = options?.onRemoveAttachment;
  }

  add(socket: RuntimeWebSocket, attachment: RealtimeSocketAttachment): void {
    const existingAttachment = this.socketStates.get(socket)?.attachment;
    if (
      existingAttachment &&
      existingAttachment.connectionKey !== attachment.connectionKey
    ) {
      this.removeSocketFromConnectionIndex(
        socket,
        existingAttachment.connectionKey
      );
    }

    this.sockets.add(socket);
    this.setAttachment(socket, attachment);
    const sockets =
      this.socketsByConnectionKey.get(attachment.connectionKey) ??
      new Set<RuntimeWebSocket>();
    sockets.add(socket);
    this.socketsByConnectionKey.set(attachment.connectionKey, sockets);
  }

  remove(socket: RuntimeWebSocket): RealtimeSocketAttachment | undefined {
    this.sockets.delete(socket);
    const attachment = this.getAttachment(socket);
    if (attachment) {
      this.onRemoveAttachment?.(attachment);
      this.removeSocketFromConnectionIndex(socket, attachment.connectionKey);
    }
    this.socketStates.delete(socket);
    return attachment;
  }

  getAttachment(
    socket: RuntimeWebSocket
  ): RealtimeSocketAttachment | undefined {
    return (
      this.socketStates.get(socket)?.attachment ??
      parseRealtimeSocketAttachment(socket.deserializeAttachment?.()) ??
      undefined
    );
  }

  ensureAttachment(socket: RuntimeWebSocket): RealtimeSocketAttachment {
    const attachment = this.getAttachment(socket);
    if (attachment) {
      this.add(socket, attachment);
      return attachment;
    }

    this.closeExpiredSession(socket);
    throw new InternalRuntimeError("Realtime socket session expired");
  }

  closeExpiredSession(socket: RuntimeWebSocket): void {
    try {
      socket.close?.(1011, "Session expired, please reconnect");
    } catch {
      // Best-effort close: the socket may already be closing.
    }
    this.remove(socket);
  }

  addSubscription(
    socket: RuntimeWebSocket,
    subscription: RealtimeSocketSubscription
  ): void {
    const attachment = this.getAttachment(socket);
    if (!attachment) {
      return;
    }

    const subscriptions = attachment.subscriptions.filter(
      (existing) => existing.subscriptionId !== subscription.subscriptionId
    );
    this.setAttachment(socket, {
      ...attachment,
      subscriptions: [...subscriptions, subscription],
    });
  }

  removeSubscription(
    socket: RuntimeWebSocket,
    subscriptionId: string
  ):
    | {
        readonly attachment: RealtimeSocketAttachment;
        readonly subscription: RealtimeSocketSubscription;
      }
    | undefined {
    const attachment = this.getAttachment(socket);
    if (!attachment) {
      return undefined;
    }

    const subscription = attachment.subscriptions.find(
      (candidate) => candidate.subscriptionId === subscriptionId
    );
    this.setAttachment(socket, {
      ...attachment,
      subscriptions: attachment.subscriptions.filter(
        (candidate) => candidate.subscriptionId !== subscriptionId
      ),
    });

    return subscription ? { attachment, subscription } : undefined;
  }

  getSubscription(
    socket: RuntimeWebSocket,
    subscriptionId: string
  ): RealtimeSocketSubscription | undefined {
    return this.getAttachment(socket)?.subscriptions.find(
      (subscription) => subscription.subscriptionId === subscriptionId
    );
  }

  updateDeliveredSequence(socket: RuntimeWebSocket, sequence: unknown): void {
    if (sequence == null || !Number.isSafeInteger(sequence)) {
      return;
    }

    const attachment = this.getAttachment(socket);
    if (!attachment) {
      return;
    }

    this.setAttachment(socket, {
      ...attachment,
      latestDeliveredOutboxSequence: Math.max(
        attachment.latestDeliveredOutboxSequence ?? 0,
        sequence as number
      ),
    });
  }

  updateSubscriptionShardName(
    connectionKey: string,
    subscriptionId: string,
    subscriptionShardName: string
  ): RealtimeSocketSubscriptionUpdate[] {
    const sockets = this.socketsByConnectionKey.get(connectionKey);
    if (!sockets) {
      return [];
    }

    const updates: RealtimeSocketSubscriptionUpdate[] = [];
    for (const socket of sockets) {
      const attachment = this.getAttachment(socket);
      const previousSubscription = attachment?.subscriptions.find(
        (subscription) => subscription.subscriptionId === subscriptionId
      );
      if (!(attachment && previousSubscription)) {
        continue;
      }

      const nextAttachment = {
        ...attachment,
        subscriptions: attachment.subscriptions.map((subscription) =>
          subscription.subscriptionId === subscriptionId
            ? { ...subscription, subscriptionShardName }
            : subscription
        ),
      };
      const nextSubscription = nextAttachment.subscriptions.find(
        (subscription) => subscription.subscriptionId === subscriptionId
      );
      if (!nextSubscription) {
        continue;
      }

      this.setAttachment(socket, nextAttachment);
      updates.push({
        nextAttachment,
        nextSubscription,
        previousAttachment: attachment,
        previousSubscription,
      });
    }

    return updates;
  }

  deliver(
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

  hasSockets(connectionKey: string): boolean {
    return (this.socketsByConnectionKey.get(connectionKey)?.size ?? 0) > 0;
  }

  hasActiveSubscriptions(): boolean {
    return [...this.socketStates.values()].some(
      ({ attachment }) => attachment.subscriptions.length > 0
    );
  }

  attachedSubscriptions(): Array<{
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

  snapshotForTest(): RealtimeSocketRegistrySnapshot {
    return {
      socketStates: this.socketStates,
      sockets: this.sockets,
      socketsByConnectionKey: this.socketsByConnectionKey,
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
        this.updateDeliveredSequence(socket, message.sequence);
        delivered += 1;
      } catch {
        this.remove(socket);
      }
    }

    return delivered;
  }

  private removeSocketFromConnectionIndex(
    socket: RuntimeWebSocket,
    connectionKey: string
  ): void {
    const sockets = this.socketsByConnectionKey.get(connectionKey);
    sockets?.delete(socket);
    if (sockets?.size === 0) {
      this.socketsByConnectionKey.delete(connectionKey);
    }
  }

  private setAttachment(
    socket: RuntimeWebSocket,
    attachment: RealtimeSocketAttachment
  ): void {
    this.socketStates.set(socket, { attachment });
    socket.serializeAttachment?.(attachment);
  }
}
