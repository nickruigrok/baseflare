import type {
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectStub,
} from "./types";

export class FakeDurableObjectNamespace implements DurableObjectNamespace {
  readonly requests: Array<{ name: string; request: Request }> = [];
  private readonly handler: (
    name: string,
    request: Request
  ) => Promise<Response>;

  constructor(handler?: (name: string, request: Request) => Promise<Response>) {
    this.handler =
      handler ?? (() => Promise.resolve(Response.json({ ok: true })));
  }

  get(id: DurableObjectId): DurableObjectStub {
    const name = id.name ?? "unknown";
    return {
      fetch: async (input, init) => {
        const request =
          input instanceof Request ? input : new Request(input, init);
        this.requests.push({ name, request });
        return await this.handler(name, request);
      },
    };
  }

  idFromName(name: string): DurableObjectId {
    return { name };
  }
}

export type AttachedTestWebSocket = WebSocket & {
  accept(): void;
  deserializeAttachment?: () => unknown;
  serializeAttachment?: (attachment: unknown) => void;
};

export class FakeRealtimeDurableObjectState {
  readonly acceptedSockets: AttachedTestWebSocket[] = [];
  readonly attachments = new WeakMap<AttachedTestWebSocket, unknown>();
  readonly durableStorage = new Map<string, unknown>();
  private readonly hibernatedSockets: readonly AttachedTestWebSocket[];
  alarmTime: number | null = null;
  failedStoragePuts = 0;

  constructor(hibernatedSockets: readonly AttachedTestWebSocket[] = []) {
    this.hibernatedSockets = hibernatedSockets;
  }

  acceptWebSocket(socket: AttachedTestWebSocket): void {
    socket.serializeAttachment = (attachment: unknown) => {
      this.attachments.set(socket, attachment);
    };
    socket.deserializeAttachment = () => this.attachments.get(socket);
    this.acceptedSockets.push(socket);
    socket.accept?.();
  }

  getWebSockets(): AttachedTestWebSocket[] {
    return [...this.hibernatedSockets, ...this.acceptedSockets];
  }

  storage = {
    delete: (key: string) => {
      this.durableStorage.delete(key);
      return Promise.resolve();
    },
    deleteAlarm: () => {
      this.alarmTime = null;
      return Promise.resolve();
    },
    get: <T = unknown>(key: string) =>
      Promise.resolve(this.durableStorage.get(key) as T | undefined),
    getAlarm: () => Promise.resolve(this.alarmTime),
    list: <T = unknown>(options?: { readonly prefix?: string }) => {
      const entries = Array.from(this.durableStorage.entries()).filter(
        ([key]) => !options?.prefix || key.startsWith(options.prefix)
      );
      return Promise.resolve(new Map(entries) as Map<string, T>);
    },
    put: <T = unknown>(key: string, value: T) => {
      if (this.failedStoragePuts > 0) {
        this.failedStoragePuts -= 1;
        return Promise.reject(new Error("Storage put failed"));
      }

      this.durableStorage.set(key, value);
      return Promise.resolve();
    },
    setAlarm: (scheduledTime: number) => {
      this.alarmTime = scheduledTime;
      return Promise.resolve();
    },
  };
}
