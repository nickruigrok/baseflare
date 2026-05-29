import { UUID, uuidv7 } from "uuidv7";

const UUID_V7_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_UUID_V7_RAND_A = 0x0f_ff;
const MAX_UUID_V7_RAND_B_HIGH = 0x3f_ff_ff_ff;
const MAX_UUID_V7_RAND_B_LOW = 0xff_ff_ff_ff;

export function isUuidV7(value: string): boolean {
  return UUID_V7_PATTERN.test(value);
}

export function generateId(): string {
  return uuidv7();
}

export function minIdForMs(milliseconds: number): string {
  return UUID.fromFieldsV7(milliseconds, 0, 0, 0).toString();
}

export function maxIdForMs(milliseconds: number): string {
  return UUID.fromFieldsV7(
    milliseconds,
    MAX_UUID_V7_RAND_A,
    MAX_UUID_V7_RAND_B_HIGH,
    MAX_UUID_V7_RAND_B_LOW
  ).toString();
}

export function getCreatedMsFromId(id: string): number {
  if (!isUuidV7(id)) {
    throw new Error(`Expected a UUIDv7 string, received "${id}"`);
  }

  const hexTimestamp = id.slice(0, 8) + id.slice(9, 13);
  return Number.parseInt(hexTimestamp, 16);
}

export function getCreatedAtFromId(id: string): Date {
  return new Date(getCreatedMsFromId(id));
}
