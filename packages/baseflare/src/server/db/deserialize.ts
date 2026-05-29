import { getCreatedMsFromId } from "baseflare/values";

const BYTES_MARKER = "$bytes";

function unescapeKey(key: string): string {
  return key.startsWith("$$") ? key.slice(1) : key;
}

function decodeBytes(value: Record<string, unknown>): Uint8Array | null {
  const keys = Object.keys(value);
  if (keys.length !== 1 || keys[0] !== BYTES_MARKER) {
    return null;
  }

  const encoded = value[BYTES_MARKER];
  if (typeof encoded !== "string") {
    return null;
  }

  return Uint8Array.from(Buffer.from(encoded, "base64"));
}

function fromStorageValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => fromStorageValue(entry));
  }

  if (typeof value === "object" && value !== null) {
    const bytes = decodeBytes(value as Record<string, unknown>);
    if (bytes) {
      return bytes;
    }

    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        unescapeKey(key),
        fromStorageValue(entry),
      ])
    );
  }

  return value;
}

export function deserialize(row: {
  _id: string;
  _data: string;
}): Record<string, unknown> & { _id: string; _createdAt: number } {
  const parsed = JSON.parse(row._data) as unknown;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("Serialized documents must deserialize into an object");
  }

  return {
    _id: row._id,
    _createdAt: getCreatedMsFromId(row._id),
    ...(fromStorageValue(parsed) as Record<string, unknown>),
  };
}
