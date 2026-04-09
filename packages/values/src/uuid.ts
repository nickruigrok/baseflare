const UUID_V7_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const BYTE_BASE = 256n;

function getRandomBytes(length: number): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(length));
}

function getTimestampByte(timestamp: bigint, byteOffset: bigint): number {
  return Number((timestamp / BYTE_BASE ** byteOffset) % BYTE_BASE);
}

function bytesToUuid(bytes: Uint8Array): string {
  const hex = Array.from(bytes, (value) =>
    value.toString(16).padStart(2, "0")
  ).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function isUuidV7(value: string): boolean {
  return UUID_V7_PATTERN.test(value);
}

export function generateId(): string {
  const bytes = getRandomBytes(16);
  const timestamp = BigInt(Date.now());
  const versionByte = bytes[6];
  const variantByte = bytes[8];

  if (versionByte === undefined || variantByte === undefined) {
    throw new Error("Expected UUID byte array to contain 16 bytes");
  }

  bytes[0] = getTimestampByte(timestamp, 5n);
  bytes[1] = getTimestampByte(timestamp, 4n);
  bytes[2] = getTimestampByte(timestamp, 3n);
  bytes[3] = getTimestampByte(timestamp, 2n);
  bytes[4] = getTimestampByte(timestamp, 1n);
  bytes[5] = getTimestampByte(timestamp, 0n);
  bytes[6] = (versionByte & 0x0f) | 0x70;
  bytes[8] = (variantByte & 0x3f) | 0x80;

  return bytesToUuid(bytes);
}

export function getCreatedAtFromId(id: string): Date {
  if (!isUuidV7(id)) {
    throw new Error(`Expected a UUIDv7 string, received "${id}"`);
  }

  const hexTimestamp = id.slice(0, 8) + id.slice(9, 13);
  const milliseconds = Number.parseInt(hexTimestamp, 16);
  return new Date(milliseconds);
}
