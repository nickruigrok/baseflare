import type { RealtimeStorage } from "./types";

const REALTIME_STORAGE_LIST_PAGE_SIZE = 128;

export async function listRealtimeStoragePrefix<T>(
  storage: RealtimeStorage,
  prefix: string
): Promise<Map<string, T>> {
  const entries = new Map<string, T>();
  let startAfter: string | undefined;

  while (true) {
    const page = await storage.list<T>({
      limit: REALTIME_STORAGE_LIST_PAGE_SIZE,
      prefix,
      startAfter,
    });
    if (page.size === 0) {
      return entries;
    }

    for (const [key, value] of page) {
      entries.set(key, value);
      startAfter = key;
    }

    if (page.size < REALTIME_STORAGE_LIST_PAGE_SIZE) {
      return entries;
    }
  }
}
