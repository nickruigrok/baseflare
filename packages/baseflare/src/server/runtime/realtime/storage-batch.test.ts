import { describe, expect, it } from "vitest";

import {
  type RealtimeStorageBatchOperation,
  writeRealtimeStorageBatch,
} from "./storage-batch";

function createRecordingStorage() {
  const deleteCalls: string[][] = [];
  const putCalls: Record<string, unknown>[] = [];
  return {
    deleteCalls,
    putCalls,
    storage: {
      delete(keys: readonly string[] | string) {
        deleteCalls.push(Array.isArray(keys) ? [...keys] : [keys]);
        return Promise.resolve();
      },
      put(keyOrEntries: Record<string, unknown> | string, value?: unknown) {
        putCalls.push(
          typeof keyOrEntries === "string"
            ? { [keyOrEntries]: value }
            : keyOrEntries
        );
        return Promise.resolve();
      },
    },
  };
}

describe("writeRealtimeStorageBatch", () => {
  it("writes puts and deletes from one batch", async () => {
    const { deleteCalls, putCalls, storage } = createRecordingStorage();

    await writeRealtimeStorageBatch(storage, [
      { key: "a", type: "put", value: 1 },
      { key: "b", type: "delete" },
      { key: "c", type: "put", value: 2 },
    ]);

    expect(putCalls).toEqual([{ a: 1, c: 2 }]);
    expect(deleteCalls).toEqual([["b"]]);
  });

  it("chunks puts and deletes to the 128-key storage limit", async () => {
    const { deleteCalls, putCalls, storage } = createRecordingStorage();
    const operations: RealtimeStorageBatchOperation[] = [];
    for (let index = 0; index < 200; index += 1) {
      operations.push({ key: `put-${index}`, type: "put", value: index });
      operations.push({ key: `delete-${index}`, type: "delete" });
    }

    await writeRealtimeStorageBatch(storage, operations);

    expect(putCalls.map((call) => Object.keys(call).length)).toEqual([128, 72]);
    expect(deleteCalls.map((call) => call.length)).toEqual([128, 72]);
    expect(putCalls.flatMap((call) => Object.keys(call))).toHaveLength(200);
  });

  it("skips storage writes for empty batches", async () => {
    const { deleteCalls, putCalls, storage } = createRecordingStorage();

    await writeRealtimeStorageBatch(storage, []);
    await writeRealtimeStorageBatch(undefined, [
      { key: "a", type: "put", value: 1 },
    ]);

    expect(putCalls).toEqual([]);
    expect(deleteCalls).toEqual([]);
  });
});
