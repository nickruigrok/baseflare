export interface BearerAuthIdentity {
  readonly token: string;
  readonly type: "bearer";
}

function parseAuthorizationHeader(headers: Headers): string | null {
  const value = headers.get("authorization");
  if (!value) {
    return null;
  }

  const match = /^Bearer ([^\s]+)$/i.exec(value.trim());
  return match?.[1] ?? null;
}

export function createAuth(headers: Headers): {
  getUserIdentity(): BearerAuthIdentity | null;
} {
  const token = parseAuthorizationHeader(headers);
  const identity: BearerAuthIdentity | null = token
    ? { token, type: "bearer" }
    : null;

  return {
    getUserIdentity() {
      return identity;
    },
  };
}
