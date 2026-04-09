import { getCreatedAtFromId } from "@baseflare/values";

const BYTE_ARRAY_MARKER = "__baseflare_bytes";

function fromStorageValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => fromStorageValue(entry));
  }

  if (typeof value === "object" && value !== null) {
    if (
      BYTE_ARRAY_MARKER in value &&
      Object.keys(value).length === 1 &&
      typeof value[BYTE_ARRAY_MARKER as keyof typeof value] === "string"
    ) {
      return Uint8Array.from(
        Buffer.from(
          value[BYTE_ARRAY_MARKER as keyof typeof value] as string,
          "base64"
        )
      );
    }

    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        fromStorageValue(entry),
      ])
    );
  }

  return value;
}

export function deserialize(row: {
  _id: string;
  _data: string;
}): Record<string, unknown> & { _id: string; _createdAt: Date } {
  const parsed = JSON.parse(row._data) as unknown;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("Serialized documents must deserialize into an object");
  }

  return {
    _id: row._id,
    _createdAt: getCreatedAtFromId(row._id),
    ...(fromStorageValue(parsed) as Record<string, unknown>),
  };
}
