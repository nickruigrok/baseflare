import { describe, expect, it } from "vitest";
import { createRealtimeActiveQueryKey } from "./evaluation-key";
import { createRegistrationKey } from "./routing";
import type { StoredRealtimeRegistration } from "./types";

function registration(
  overrides: Partial<StoredRealtimeRegistration> = {}
): StoredRealtimeRegistration {
  return {
    args: { filter: { done: false, ownerToken: "owner-a" } },
    authorizationFingerprint: "auth-owner-a",
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

async function activeQueryKey(
  registration: StoredRealtimeRegistration
): Promise<string> {
  return await createRealtimeActiveQueryKey(
    registration,
    createRegistrationKey(
      registration.connectionKey,
      registration.subscriptionId
    )
  );
}

describe("realtime evaluation keys", () => {
  it("canonicalizes object args independent of key insertion order", async () => {
    const first = registration({
      args: { a: 1, nested: { x: true, y: "yes" } },
    });
    const second = registration({
      args: { nested: { y: "yes", x: true }, a: 1 },
      connectionKey: "client:client-b",
      subscriptionId: "sub-b",
    });

    await expect(activeQueryKey(first)).resolves.toBe(
      await activeQueryKey(second)
    );
  });

  it("keeps authorization contexts separate", async () => {
    const first = registration({ authorizationFingerprint: "auth-owner-a" });
    const second = registration({
      authorizationFingerprint: "auth-owner-b",
      connectionKey: "client:client-b",
      subscriptionId: "sub-b",
    });

    await expect(activeQueryKey(first)).resolves.not.toBe(
      await activeQueryKey(second)
    );
  });

  it("uses per-registration keys for unsafe args", async () => {
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

    await expect(activeQueryKey(first)).resolves.toBe(
      await activeQueryKey(second)
    );
    await expect(activeQueryKey(unsafe)).resolves.toMatch(/^aq:[0-9a-f]{64}$/);
    await expect(activeQueryKey(unsafe)).resolves.not.toContain("client-c");
  });

  it("does not expose bearer tokens in active query keys", async () => {
    const key = await activeQueryKey(
      registration({ authorizationFingerprint: "Bearer owner-a" })
    );

    expect(key).toMatch(/^aq:[0-9a-f]{64}$/);
    expect(key).not.toContain("Bearer");
    expect(key).not.toContain("owner-a");
  });
});
