export interface BearerAuthIdentity {
  readonly token: string;
  readonly type: "bearer";
}

export function createAuth(_headers: Headers): {
  getUserIdentity(): BearerAuthIdentity | null;
} {
  // Phase 5 will install Better Auth-backed identity resolution here. Until
  // then, raw bearer strings are opaque credentials, not verified identity.
  return {
    getUserIdentity() {
      return null;
    },
  };
}
