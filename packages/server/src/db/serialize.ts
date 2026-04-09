const BYTE_ARRAY_MARKER = "__baseflare_bytes";

function toStorageValue(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return { [BYTE_ARRAY_MARKER]: Buffer.from(value).toString("base64") };
  }

  if (Array.isArray(value)) {
    return value.map((entry) => toStorageValue(entry));
  }

  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, toStorageValue(entry)])
    );
  }

  return value;
}

export function serialize(doc: Record<string, unknown>): { _data: string } {
  const payload: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(doc)) {
    if (key === "_id" || key === "_createdAt") {
      continue;
    }

    payload[key] = toStorageValue(value);
  }

  return { _data: JSON.stringify(payload) };
}
