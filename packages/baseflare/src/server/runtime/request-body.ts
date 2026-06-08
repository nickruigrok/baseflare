import { PayloadTooLargeRuntimeError, ValidationRuntimeError } from "./errors";

export const MAX_RPC_JSON_DEPTH = 32;
export const MAX_RPC_JSON_NODES = 10_000;
export const MAX_RPC_JSON_STRING_LENGTH = 16 * 1024;
export const MAX_RPC_JSON_COLLECTION_LENGTH = 10_000;

export async function readRequestBodyText(
  request: Request,
  maxBytes: number
): Promise<string> {
  if (!request.body) {
    return "";
  }

  const reader = request.body.getReader();
  const decoder = new TextDecoder();
  let totalBytes = 0;
  let bodyText = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      totalBytes += value.byteLength;
      if (totalBytes > maxBytes) {
        await reader.cancel().catch(() => undefined);
        throw new PayloadTooLargeRuntimeError();
      }

      bodyText += decoder.decode(value, { stream: true });
    }

    bodyText += decoder.decode();
    return bodyText;
  } finally {
    reader.releaseLock();
  }
}

export function assertRpcJsonBounds(value: unknown, label = "RPC value"): void {
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
    if (nodeCount > MAX_RPC_JSON_NODES) {
      throw new ValidationRuntimeError(
        `${label} must contain at most ${MAX_RPC_JSON_NODES} JSON nodes`
      );
    }

    if (current.depth > MAX_RPC_JSON_DEPTH) {
      throw new ValidationRuntimeError(
        `${label} must be at most ${MAX_RPC_JSON_DEPTH} levels deep`
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
    assertStringLength(value, label, MAX_RPC_JSON_STRING_LENGTH);
    return;
  }

  if (Array.isArray(value)) {
    if (value.length > MAX_RPC_JSON_COLLECTION_LENGTH) {
      throw new ValidationRuntimeError(
        `${label} arrays must contain at most ${MAX_RPC_JSON_COLLECTION_LENGTH} items`
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
  if (entries.length > MAX_RPC_JSON_COLLECTION_LENGTH) {
    throw new ValidationRuntimeError(
      `${label} objects must contain at most ${MAX_RPC_JSON_COLLECTION_LENGTH} keys`
    );
  }
  for (const [key, child] of entries) {
    assertStringLength(key, `${label} key`, MAX_RPC_JSON_STRING_LENGTH);
    stack.push({ depth: depth + 1, value: child });
  }
}

function assertStringLength(
  value: string,
  label: string,
  maxLength: number
): void {
  if (value.length > maxLength) {
    throw new ValidationRuntimeError(
      `${label} must be at most ${maxLength} characters`
    );
  }
}
