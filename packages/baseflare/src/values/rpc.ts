export interface QueryRequest {
  args: Record<string, unknown>;
  name: string;
}

export interface MutationRequest {
  args: Record<string, unknown>;
  name: string;
}

export interface ActionRequest {
  args: Record<string, unknown>;
  name: string;
}

export interface RPCResponse<TResult> {
  result: TResult;
}

export interface RPCError {
  code: string;
  data?: unknown;
  message: string;
}

export interface WSSubscribeMessage {
  args: Record<string, unknown>;
  query: string;
  subscriptionId: string;
  type: "subscribe";
}

export interface WSUnsubscribeMessage {
  subscriptionId: string;
  type: "unsubscribe";
}

export interface WSResultEvent {
  data: unknown;
  subscriptionId: string;
  type: "result";
}

export interface WSErrorEvent {
  message: string;
  subscriptionId: string;
  type: "error";
}

export interface WSHeartbeatEvent {
  timestamp: number;
  type: "heartbeat";
}
