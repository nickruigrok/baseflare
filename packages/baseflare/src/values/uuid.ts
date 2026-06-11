import { UUID, uuidv7 } from "uuidv7";

import { ValidationError } from "./errors";

const UUID_V7_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_UUID_V7_TIMESTAMP_MS = 0xff_ff_ff_ff_ff_ff;
const MAX_UUID_V7_RAND_A = 0x0f_ff;
const MAX_UUID_V7_RAND_B_HIGH = 0x3f_ff_ff_ff;
const MAX_UUID_V7_RAND_B_LOW = 0xff_ff_ff_ff;

function assertUuidV7Timestamp(milliseconds: number): void {
  if (
    !Number.isSafeInteger(milliseconds) ||
    milliseconds < 0 ||
    milliseconds > MAX_UUID_V7_TIMESTAMP_MS
  ) {
    throw new ValidationError(
      "milliseconds",
      `milliseconds must be an integer between 0 and ${MAX_UUID_V7_TIMESTAMP_MS}`
    );
  }
}

/** Returns true when `value` is a well-formed UUIDv7 string (Baseflare document id format). */
export function isUuidV7(value: string): boolean {
  return UUID_V7_PATTERN.test(value);
}

/** Generates a new time-sortable UUIDv7 document id. */
export function generateId(): string {
  return uuidv7();
}

/**
 * Returns the smallest possible document id for the given Unix-millisecond
 * timestamp. Useful for time-range filters on `_id`.
 */
export function minIdForMs(milliseconds: number): string {
  assertUuidV7Timestamp(milliseconds);
  return UUID.fromFieldsV7(milliseconds, 0, 0, 0).toString();
}

/**
 * Returns the largest possible document id for the given Unix-millisecond
 * timestamp. Useful for time-range filters on `_id`.
 */
export function maxIdForMs(milliseconds: number): string {
  assertUuidV7Timestamp(milliseconds);
  return UUID.fromFieldsV7(
    milliseconds,
    MAX_UUID_V7_RAND_A,
    MAX_UUID_V7_RAND_B_HIGH,
    MAX_UUID_V7_RAND_B_LOW
  ).toString();
}

/** Extracts the creation time in Unix milliseconds embedded in a UUIDv7 document id. */
export function getCreatedMsFromId(id: string): number {
  if (!isUuidV7(id)) {
    throw new Error(`Expected a UUIDv7 string, received "${id}"`);
  }

  const hexTimestamp = id.slice(0, 8) + id.slice(9, 13);
  return Number.parseInt(hexTimestamp, 16);
}

/** Extracts the creation time embedded in a UUIDv7 document id as a Date. */
export function getCreatedAtFromId(id: string): Date {
  return new Date(getCreatedMsFromId(id));
}
