import { getRealtimeRegistrationHomeRouteTarget } from "./routing";
import type {
  RealtimeSubscriptionRouteTarget,
  StoredRealtimeRegistration,
} from "./types";

function canonicalizeRealtimeValue(value: unknown): string | null {
  if (value === null) {
    return "null";
  }

  if (typeof value === "string") {
    return JSON.stringify(value);
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? JSON.stringify(value) : null;
  }

  if (Array.isArray(value)) {
    return canonicalizeRealtimeArray(value);
  }

  if (typeof value !== "object" || value === undefined) {
    return null;
  }

  const prototype = Object.getPrototypeOf(value);
  if (!(prototype === Object.prototype || prototype === null)) {
    return null;
  }

  const input = value as Record<string, unknown>;
  return canonicalizeRealtimeObject(input);
}

function canonicalizeRealtimeArray(values: readonly unknown[]): string | null {
  const items: string[] = [];
  for (const value of values) {
    const canonicalValue = canonicalizeRealtimeValue(value);
    if (canonicalValue === null) {
      return null;
    }
    items.push(canonicalValue);
  }

  return `[${items.join(",")}]`;
}

function canonicalizeRealtimeObject(
  input: Record<string, unknown>
): string | null {
  const entries: string[] = [];
  for (const key of Object.keys(input).sort()) {
    const canonicalValue = canonicalizeRealtimeValue(input[key]);
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

function createRealtimeEvaluationKey(
  registration: StoredRealtimeRegistration
): string | null {
  const args = canonicalizeRealtimeValue(registration.args);
  if (args === null) {
    return null;
  }

  return JSON.stringify([
    registration.runtimeId,
    registration.queryName,
    args,
    registration.authorizationHeader ?? null,
    getRouteKey(
      getRealtimeRegistrationHomeRouteTarget(registration.dependencies)
    ),
  ]);
}

export function createRealtimeActiveQueryKey(
  registration: StoredRealtimeRegistration,
  registrationKey: string
): string {
  return (
    createRealtimeEvaluationKey(registration) ??
    JSON.stringify(["registration", registrationKey])
  );
}
