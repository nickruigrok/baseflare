import { sha256Hex } from "./hash";
import { getRealtimeRegistrationHomeRouteTarget } from "./routing";
import type {
  RealtimeSubscriptionRouteTarget,
  StoredRealtimeRegistration,
} from "./types";
import {
  REALTIME_MAX_JSON_DEPTH,
  REALTIME_MAX_JSON_NODES,
  REALTIME_MAX_JSON_STRING_LENGTH,
} from "./types";

interface CanonicalizationState {
  nodeCount: number;
}

function canonicalizeRealtimeValue(
  value: unknown,
  state: CanonicalizationState,
  depth = 0
): string | null {
  state.nodeCount += 1;
  if (state.nodeCount > REALTIME_MAX_JSON_NODES) {
    return null;
  }
  if (depth > REALTIME_MAX_JSON_DEPTH) {
    return null;
  }

  if (value === null) {
    return "null";
  }

  if (typeof value === "string") {
    if (value.length > REALTIME_MAX_JSON_STRING_LENGTH) {
      return null;
    }
    return JSON.stringify(value);
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? JSON.stringify(value) : null;
  }

  if (Array.isArray(value)) {
    return canonicalizeRealtimeArray(value, state, depth);
  }

  if (typeof value !== "object") {
    return null;
  }

  const prototype = Object.getPrototypeOf(value);
  if (!(prototype === Object.prototype || prototype === null)) {
    return null;
  }

  const input = value as Record<string, unknown>;
  return canonicalizeRealtimeObject(input, state, depth);
}

function canonicalizeRealtimeArray(
  values: readonly unknown[],
  state: CanonicalizationState,
  depth: number
): string | null {
  const items: string[] = [];
  for (const value of values) {
    const canonicalValue = canonicalizeRealtimeValue(value, state, depth + 1);
    if (canonicalValue === null) {
      return null;
    }
    items.push(canonicalValue);
  }

  return `[${items.join(",")}]`;
}

function canonicalizeRealtimeObject(
  input: Record<string, unknown>,
  state: CanonicalizationState,
  depth: number
): string | null {
  const entries: string[] = [];
  for (const key of Object.keys(input).sort()) {
    if (key.length > REALTIME_MAX_JSON_STRING_LENGTH) {
      return null;
    }
    const canonicalValue = canonicalizeRealtimeValue(
      input[key],
      state,
      depth + 1
    );
    if (canonicalValue === null) {
      return null;
    }
    entries.push(`${JSON.stringify(key)}:${canonicalValue}`);
  }

  return `{${entries.join(",")}}`;
}

function getRouteKey(route: RealtimeSubscriptionRouteTarget): string {
  if (route.type === "global") {
    return "global";
  }

  if (route.type === "table") {
    return `table:${route.tableName}`;
  }

  return `partition:${route.partition.tableName}:${route.partition.partitionKey}:${route.partition.partitionValue}`;
}

async function createRealtimeActiveQueryHash(
  registration: StoredRealtimeRegistration
): Promise<string | null> {
  const args = canonicalizeRealtimeValue(registration.args, { nodeCount: 0 });
  if (args === null) {
    return null;
  }

  const authorizationFingerprint =
    registration.authorizationFingerprint ?? null;
  return await sha256Hex(
    JSON.stringify([
      registration.runtimeId,
      registration.queryName,
      args,
      authorizationFingerprint,
      getRouteKey(
        getRealtimeRegistrationHomeRouteTarget(registration.dependencies)
      ),
    ])
  );
}

export async function createRealtimeActiveQueryKey(
  registration: StoredRealtimeRegistration,
  registrationKey: string
): Promise<string> {
  const evaluationKey = await createRealtimeActiveQueryHash(registration);
  return `aq:${
    evaluationKey ??
    (await sha256Hex(JSON.stringify(["registration", registrationKey])))
  }`;
}

export function isRealtimeActiveQueryKey(
  value: string | undefined
): value is string {
  return typeof value === "string" && value.startsWith("aq:");
}
