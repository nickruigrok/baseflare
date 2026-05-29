const BYTES_MARKER = "$bytes";
const RESERVED_DOCUMENT_FIELDS = new Set(["_id", "_createdAt"]);

/** User keys starting with `$` are escaped (doubled) to avoid colliding with reserved markers. */
function escapeKey(key: string): string {
  return key.startsWith("$") ? `$${key}` : key;
}

function toStorageValue(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return { [BYTES_MARKER]: Buffer.from(value).toString("base64") };
  }

  if (Array.isArray(value)) {
    return value.map((entry) => toStorageValue(entry));
  }

  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        escapeKey(key),
        toStorageValue(entry),
      ])
    );
  }

  return value;
}

export function serialize(doc: Record<string, unknown>): { _data: string } {
  const payload: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(doc)) {
    if (RESERVED_DOCUMENT_FIELDS.has(key)) {
      continue;
    }

    payload[escapeKey(key)] = toStorageValue(value);
  }

  return { _data: JSON.stringify(payload) };
}
