import type {
  Scheduler,
  StorageActionWriter,
  StorageReader,
  StorageWriter,
} from "../functions/types";

import { NotImplementedRuntimeError } from "./errors";

function notImplemented(feature: string): never {
  throw new NotImplementedRuntimeError(
    `${feature} is not implemented yet in Baseflare runtime`
  );
}

export function createStorageReaderPlaceholder(): StorageReader {
  return {
    getUrl() {
      return notImplemented("Storage");
    },
  };
}

export function createStorageWriterPlaceholder(): StorageWriter {
  return {
    ...createStorageReaderPlaceholder(),
    delete() {
      return notImplemented("Storage");
    },
    generateUploadUrl() {
      return notImplemented("Storage");
    },
  };
}

export function createStorageActionWriterPlaceholder(): StorageActionWriter {
  return {
    ...createStorageWriterPlaceholder(),
    store() {
      return notImplemented("Storage");
    },
  };
}

export function createSchedulerPlaceholder(): Scheduler {
  return {
    runAfter() {
      return notImplemented("Scheduler");
    },
    runAt() {
      return notImplemented("Scheduler");
    },
  };
}
