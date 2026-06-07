import { describe, expect, it } from "vitest";
import { createRealtimeActiveQueryKey } from "./evaluation-key";
import { createRegistrationKey } from "./routing";
import type { StoredRealtimeRegistration } from "./types";

function registration(
  overrides: Partial<StoredRealtimeRegistration> = {}
): StoredRealtimeRegistration {
  return {
    args: { filter: { done: false, ownerToken: "owner-a" } },
    authorizationHeader: "Bearer owner-a",
    connectionKey: "client:client-a",
    connectionName: "connection:0",
    epoch: 1,
    leaseExpiresAt: Date.now() + 60_000,
    queryName: "todos:list",
    runtimeId: "runtime:1",
    subscriptionId: "sub-a",
    ...overrides,
  };
}

function activeQueryKey(registration: StoredRealtimeRegistration): string {
  return createRealtimeActiveQueryKey(
    registration,
    createRegistrationKey(
      registration.connectionKey,
      registration.subscriptionId
    )
  );
}

describe("realtime evaluation keys", () => {
  it("canonicalizes object args independent of key insertion order", () => {
    const first = registration({
      args: { a: 1, nested: { x: true, y: "yes" } },
    });
    const second = registration({
      args: { nested: { y: "yes", x: true }, a: 1 },
      connectionKey: "client:client-b",
      subscriptionId: "sub-b",
    });

    expect(activeQueryKey(first)).toBe(activeQueryKey(second));
  });

  it("keeps authorization contexts separate", () => {
    const first = registration({ authorizationHeader: "Bearer owner-a" });
    const second = registration({
      authorizationHeader: "Bearer owner-b",
      connectionKey: "client:client-b",
      subscriptionId: "sub-b",
    });

    expect(activeQueryKey(first)).not.toBe(activeQueryKey(second));
  });

  it("uses per-registration keys for unsafe args", () => {
    const first = registration();
    const second = registration({
      connectionKey: "client:client-b",
      subscriptionId: "sub-b",
    });
    const unsafe = registration({
      args: { ownerToken: "owner-a", value: Number.NaN },
      connectionKey: "client:client-c",
      subscriptionId: "sub-c",
    });

    expect(activeQueryKey(first)).toBe(activeQueryKey(second));
    expect(activeQueryKey(unsafe)).toBe(
      JSON.stringify([
        "registration",
        createRegistrationKey("client:client-c", "sub-c"),
      ])
    );
  });
});
