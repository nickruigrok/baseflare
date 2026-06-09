import { ValidationRuntimeError } from "./errors";

export const MAX_JSON_DEPTH = 32;
export const MAX_JSON_NODES = 10_000;
export const MAX_JSON_STRING_LENGTH = 16 * 1024;
export const MAX_JSON_COLLECTION_LENGTH = 10_000;

/**
 * Validates that untrusted JSON stays within depth, node, string, and
 * collection bounds. Shared by RPC request bodies and realtime messages.
 */
export function assertJsonBounds(value: unknown, label: string): void {
  let nodeCount = 0;
  const stack: Array<{ readonly depth: number; readonly value: unknown }> = [
    { depth: 0, value },
  ];

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) {
      continue;
    }

    nodeCount += 1;
    if (nodeCount > MAX_JSON_NODES) {
      throw new ValidationRuntimeError(
        `${label} must contain at most ${MAX_JSON_NODES} JSON nodes`
      );
    }

    if (current.depth > MAX_JSON_DEPTH) {
      throw new ValidationRuntimeError(
        `${label} must be at most ${MAX_JSON_DEPTH} levels deep`
      );
    }

    pushJsonChildren(current.value, current.depth, label, stack);
  }
}

function pushJsonChildren(
  value: unknown,
  depth: number,
  label: string,
  stack: Array<{ readonly depth: number; readonly value: unknown }>
): void {
  if (typeof value === "string") {
    assertBoundedStringLength(value, label);
    return;
  }

  if (Array.isArray(value)) {
    if (value.length > MAX_JSON_COLLECTION_LENGTH) {
      throw new ValidationRuntimeError(
        `${label} arrays must contain at most ${MAX_JSON_COLLECTION_LENGTH} items`
      );
    }
    for (const item of value) {
      stack.push({ depth: depth + 1, value: item });
    }
    return;
  }

  if (typeof value !== "object" || value === null) {
    return;
  }

  const prototype = Object.getPrototypeOf(value);
  if (!(prototype === Object.prototype || prototype === null)) {
    throw new ValidationRuntimeError(`${label} must be JSON-serializable`);
  }

  const entries = Object.entries(value as Record<string, unknown>);
  if (entries.length > MAX_JSON_COLLECTION_LENGTH) {
    throw new ValidationRuntimeError(
      `${label} objects must contain at most ${MAX_JSON_COLLECTION_LENGTH} keys`
    );
  }
  for (const [key, child] of entries) {
    assertBoundedStringLength(key, `${label} key`);
    stack.push({ depth: depth + 1, value: child });
  }
}

function assertBoundedStringLength(value: string, label: string): void {
  if (value.length > MAX_JSON_STRING_LENGTH) {
    throw new ValidationRuntimeError(
      `${label} must be at most ${MAX_JSON_STRING_LENGTH} characters`
    );
  }
}
