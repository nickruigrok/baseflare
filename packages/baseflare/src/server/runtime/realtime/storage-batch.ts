import type { RealtimeStorage } from "./types";

export type RealtimeStorageBatchOperation =
  | {
      readonly key: string;
      readonly type: "delete";
    }
  | {
      readonly key: string;
      readonly type: "put";
      readonly value: unknown;
    };

// Durable Object storage accepts at most 128 keys per put()/delete() call.
const MAX_KEYS_PER_CALL = 128;

export async function writeRealtimeStorageBatch(
  storage: Pick<RealtimeStorage, "delete" | "put"> | undefined,
  operations: readonly RealtimeStorageBatchOperation[]
): Promise<void> {
  if (!storage || operations.length === 0) {
    return;
  }

  const putEntries = Object.create(null) as Record<string, unknown>;
  const deleteKeys: string[] = [];
  for (const operation of operations) {
    if (operation.type === "put") {
      Object.defineProperty(putEntries, operation.key, {
        configurable: true,
        enumerable: true,
        value: operation.value,
      });
      continue;
    }

    deleteKeys.push(operation.key);
  }

  // Issue every chunked call synchronously before awaiting: Durable Object
  // write coalescing commits all writes issued without an intervening await
  // as one atomic batch. Chunking exists only for the 128-key per-call API
  // limit and is NOT an atomicity boundary on its own - the guarantee rests
  // on no awaits between the calls below.
  const writes: Promise<void>[] = [];
  const putKeys = Object.keys(putEntries);
  for (let index = 0; index < putKeys.length; index += MAX_KEYS_PER_CALL) {
    const chunk = Object.create(null) as Record<string, unknown>;
    for (const key of putKeys.slice(index, index + MAX_KEYS_PER_CALL)) {
      Object.defineProperty(chunk, key, {
        configurable: true,
        enumerable: true,
        value: putEntries[key],
      });
    }
    writes.push(storage.put(chunk));
  }
  for (let index = 0; index < deleteKeys.length; index += MAX_KEYS_PER_CALL) {
    writes.push(
      storage.delete(deleteKeys.slice(index, index + MAX_KEYS_PER_CALL))
    );
  }

  await Promise.all(writes);
}
