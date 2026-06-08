import { describe, expect, it, vi } from "vitest";
import { createRealtimeSocketAttachment } from "./shared";
import { RealtimeSocketRegistry } from "./socket-registry";
import type {
  RealtimeSocketAttachment,
  RealtimeSocketSubscription,
  RuntimeWebSocket,
} from "./types";

type TestSocket = RuntimeWebSocket & {
  readonly sentMessages: string[];
  readonly storedAttachments: unknown[];
  failSends?: boolean;
};

function createSocket(): TestSocket {
  const storedAttachments: unknown[] = [];
  const socket = {
    accept: vi.fn(),
    addEventListener: vi.fn(),
    close: vi.fn(),
    deserializeAttachment: () => storedAttachments.at(-1),
    dispatchEvent: vi.fn(),
    failSends: false,
    readyState: 1,
    removeEventListener: vi.fn(),
    send(message: string) {
      if (socket.failSends) {
        throw new Error("socket unavailable");
      }
      socket.sentMessages.push(message);
    },
    sentMessages: [],
    serializeAttachment: (attachment: unknown) => {
      storedAttachments.push(attachment);
    },
    storedAttachments,
  } as unknown as TestSocket;
  return socket;
}

function attachment(
  connectionKey: string,
  subscriptions: readonly RealtimeSocketSubscription[] = []
): RealtimeSocketAttachment {
  return {
    ...createRealtimeSocketAttachment({
      authorizationFingerprint: undefined,
      connectionKey,
      runtimeId: "runtime:1",
    }),
    subscriptions,
  };
}

describe("RealtimeSocketRegistry", () => {
  it("removes socket state and connection indexes through one path", () => {
    const removedAttachments: RealtimeSocketAttachment[] = [];
    const registry = new RealtimeSocketRegistry({
      onRemoveAttachment: (removed) => {
        removedAttachments.push(removed);
      },
    });
    const socket = createSocket();
    const socketAttachment = attachment("client:client-a");

    registry.add(socket, socketAttachment);
    expect(registry.hasSockets("client:client-a")).toBe(true);

    registry.remove(socket);

    expect(registry.hasSockets("client:client-a")).toBe(false);
    expect(registry.snapshotForTest().socketStates.size).toBe(0);
    expect(registry.snapshotForTest().sockets.size).toBe(0);
    expect(removedAttachments).toEqual([socketAttachment]);
  });

  it("delivers only to sockets for the target connection key", () => {
    const registry = new RealtimeSocketRegistry();
    const clientSocket = createSocket();
    const otherSocket = createSocket();
    registry.add(
      clientSocket,
      attachment("client:client-a", [
        {
          args: {},
          epoch: 1,
          queryName: "todos:list",
          subscriptionId: "sub-a",
          subscriptionShardName: "subscription:g1:0",
        },
      ])
    );
    registry.add(otherSocket, attachment("client:client-b"));

    const result = registry.deliver("client:client-a", "subscription:g1:0", [
      { result: [{ id: "todo-a" }], sequence: 7, subscriptionId: "sub-a" },
    ]);

    expect(result).toEqual({
      delivered: 1,
      deliveredSubscriptions: ["sub-a"],
    });
    expect(clientSocket.sentMessages).toHaveLength(1);
    expect(otherSocket.sentMessages).toEqual([]);
    expect(
      registry.getAttachment(clientSocket)?.latestDeliveredOutboxSequence
    ).toBe(7);
  });

  it("removes failed sockets during delivery without blocking healthy sockets", () => {
    const registry = new RealtimeSocketRegistry();
    const failedSocket = createSocket();
    const healthySocket = createSocket();
    failedSocket.failSends = true;
    const subscriptions = [
      {
        args: {},
        epoch: 1,
        queryName: "todos:list",
        subscriptionId: "sub-a",
        subscriptionShardName: "subscription:g1:0",
      },
    ];
    registry.add(failedSocket, attachment("client:client-a", subscriptions));
    registry.add(healthySocket, attachment("client:client-a", subscriptions));

    const result = registry.deliver("client:client-a", "subscription:g1:0", [
      { result: [], sequence: 9, subscriptionId: "sub-a" },
    ]);

    expect(result).toEqual({
      delivered: 1,
      deliveredSubscriptions: ["sub-a"],
    });
    expect(registry.snapshotForTest().socketStates.has(failedSocket)).toBe(
      false
    );
    expect(registry.snapshotForTest().socketStates.has(healthySocket)).toBe(
      true
    );
    expect(registry.hasSockets("client:client-a")).toBe(true);
  });

  it("delivers only to sockets that own the subscription id", () => {
    const registry = new RealtimeSocketRegistry();
    const owningSocket = createSocket();
    const bucketPeerSocket = createSocket();
    registry.add(
      owningSocket,
      attachment("client:client-a", [
        {
          args: {},
          epoch: 1,
          queryName: "todos:list",
          subscriptionId: "sub-a",
          subscriptionShardName: "subscription:g1:0",
        },
      ])
    );
    registry.add(
      bucketPeerSocket,
      attachment("client:client-a", [
        {
          args: {},
          epoch: 1,
          queryName: "todos:list",
          subscriptionId: "sub-b",
          subscriptionShardName: "subscription:g1:0",
        },
      ])
    );

    const result = registry.deliver("client:client-a", "subscription:g1:0", [
      { result: [{ id: "todo-a" }], sequence: 7, subscriptionId: "sub-a" },
    ]);

    expect(result).toEqual({
      delivered: 1,
      deliveredSubscriptions: ["sub-a"],
    });
    expect(owningSocket.sentMessages).toHaveLength(1);
    expect(bucketPeerSocket.sentMessages).toEqual([]);
    expect(registry.hasSubscriptionSocket("client:client-a", "sub-a")).toBe(
      true
    );
    expect(registry.hasSubscriptionSocket("client:client-a", "sub-c")).toBe(
      false
    );
  });

  it("does not deliver from a shard that no longer owns the subscription", () => {
    const registry = new RealtimeSocketRegistry();
    const socket = createSocket();
    registry.add(
      socket,
      attachment("client:client-a", [
        {
          args: {},
          epoch: 1,
          queryName: "todos:list",
          subscriptionId: "sub-a",
          subscriptionShardName: "subscription:g1:2",
        },
      ])
    );

    const result = registry.deliver("client:client-a", "subscription:g1:0", [
      { result: [{ id: "todo-a" }], sequence: 7, subscriptionId: "sub-a" },
    ]);

    expect(result).toEqual({
      delivered: 0,
      deliveredSubscriptions: [],
    });
    expect(socket.sentMessages).toEqual([]);
  });

  it("tracks subscriptions and active subscription state in attachments", () => {
    const registry = new RealtimeSocketRegistry();
    const socket = createSocket();
    registry.add(socket, attachment("client:client-a"));
    const subscription = {
      args: { ownerToken: "owner-a" },
      epoch: 1,
      queryName: "todos:list",
      subscriptionId: "sub-a",
      subscriptionShardName: "subscription:g1:0",
    };

    registry.addSubscription(socket, subscription);

    expect(registry.hasActiveSubscriptions()).toBe(true);
    expect(registry.getSubscription(socket, "sub-a")).toEqual(subscription);
    expect(registry.attachedSubscriptions()).toEqual([
      {
        attachment: registry.getAttachment(socket),
        subscription,
      },
    ]);

    const removed = registry.removeSubscription(socket, "sub-a");

    expect(removed).toEqual({
      attachment: expect.objectContaining({
        connectionKey: "client:client-a",
      }),
      subscription,
    });
    expect(registry.hasActiveSubscriptions()).toBe(false);
  });

  it("does not serialize attachments when removing a missing subscription", () => {
    const registry = new RealtimeSocketRegistry();
    const socket = createSocket();
    const subscription = {
      args: { ownerToken: "owner-a" },
      epoch: 1,
      queryName: "todos:list",
      subscriptionId: "sub-a",
      subscriptionShardName: "subscription:g1:0",
    };
    const socketAttachment = attachment("client:client-a", [subscription]);
    registry.add(socket, socketAttachment);
    const writesBeforeRemoval = socket.storedAttachments.length;

    const removed = registry.removeSubscription(socket, "missing-sub");

    expect(removed).toBeUndefined();
    expect(socket.storedAttachments).toHaveLength(writesBeforeRemoval);
    expect(registry.getAttachment(socket)?.subscriptions).toEqual([
      subscription,
    ]);
  });

  it("updates subscription shard ownership in socket attachments", () => {
    const registry = new RealtimeSocketRegistry();
    const socket = createSocket();
    registry.add(
      socket,
      attachment("client:client-a", [
        {
          args: {},
          epoch: 1,
          queryName: "todos:list",
          subscriptionId: "sub-a",
          subscriptionShardName: "subscription:g1:0",
        },
      ])
    );

    const updates = registry.updateSubscriptionShardName(
      "client:client-a",
      "sub-a",
      "subscription:g1:4"
    );

    expect(updates).toHaveLength(1);
    expect(updates[0]?.previousSubscription.subscriptionShardName).toBe(
      "subscription:g1:0"
    );
    expect(updates[0]?.nextSubscription.subscriptionShardName).toBe(
      "subscription:g1:4"
    );
    expect(
      registry.getSubscription(socket, "sub-a")?.subscriptionShardName
    ).toBe("subscription:g1:4");
  });

  it("fails closed when a socket attachment cannot be restored", () => {
    const registry = new RealtimeSocketRegistry();
    const socket = createSocket();

    expect(() => registry.ensureAttachment(socket)).toThrow(
      "Realtime socket session expired"
    );
    expect(socket.close).toHaveBeenCalledWith(
      1011,
      "Session expired, please reconnect"
    );
    expect(registry.snapshotForTest().socketStates.size).toBe(0);
  });
});
