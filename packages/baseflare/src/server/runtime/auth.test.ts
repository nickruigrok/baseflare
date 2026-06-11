import { describe, expect, it } from "vitest";
import { createAuth } from "./auth";

// This pins the Phase 3 trust model: raw bearer headers are opaque
// credentials, never verified identity, on EVERY execution path. The realtime
// re-evaluation path relies on this parity when it evaluates with empty
// headers (see evaluateActiveQueryDefinition). Phase 5 replaces createAuth
// with Better Auth-backed identity resolution; when this test breaks, the
// realtime path must gain per-evaluation identity re-resolution in the same
// change (IMPLEMENTATION_PLAN.md Phase 5, deliverable 5).
describe("createAuth", () => {
  it("returns null identity regardless of authorization headers", () => {
    expect(createAuth(new Headers()).getUserIdentity()).toBeNull();
    expect(
      createAuth(
        new Headers({ authorization: "Bearer owner-a" })
      ).getUserIdentity()
    ).toBeNull();
  });
});
