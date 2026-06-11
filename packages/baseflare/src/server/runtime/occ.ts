import { emitRuntimeMetric, logRuntimeEvent } from "./logging";

export interface OccConflictMetricEvent {
  readonly partitionAligned: boolean;
  readonly partitioned: boolean;
  readonly scope: "partition" | "row" | "table";
  readonly table: string;
}

const OCC_CONFLICT_RETRY_METRIC = "baseflare.runtime.occ.conflict_retries";
const OCC_RETRY_EXHAUSTION_METRIC = "baseflare.runtime.occ.retry_exhaustions";
const OCC_CONTENTION_WARNING_THRESHOLD = 10;
const OCC_CONTENTION_WARNING_WINDOW_MS = 60_000;
const OCC_CONTENTION_WARNING_DEDUP_MS = 600_000;
const occContentionWarningState = new Map<
  string,
  { count: number; lastWarnedAtMs: number; windowStartedAtMs: number }
>();

declare const __BASEFLARE_DEV_WARNINGS__: boolean | undefined;

/** @internal test-only */
export function resetOccContentionWarningStateForTest(): void {
  occContentionWarningState.clear();
}

export function recordOccConflictRetryMetrics(
  events: readonly OccConflictMetricEvent[]
): void {
  for (const event of events) {
    emitOccMetric(OCC_CONFLICT_RETRY_METRIC, event);
    if (
      typeof __BASEFLARE_DEV_WARNINGS__ !== "undefined" &&
      __BASEFLARE_DEV_WARNINGS__
    ) {
      maybeWarnOccContention(event);
    }
  }
}

export function recordOccRetryExhaustionMetrics(
  events: readonly OccConflictMetricEvent[]
): void {
  for (const event of events) {
    emitOccMetric(OCC_RETRY_EXHAUSTION_METRIC, event);
  }
}

function emitOccMetric(name: string, event: OccConflictMetricEvent): void {
  emitRuntimeMetric(name, 1, {
    partitionAligned: event.partitionAligned,
    partitioned: event.partitioned,
    scope: event.scope,
    table: event.table,
  });
}

function maybeWarnOccContention(event: OccConflictMetricEvent): void {
  if (event.scope !== "table" || event.partitionAligned) {
    return;
  }

  const now = Date.now();
  const state = occContentionWarningState.get(event.table);
  const currentState =
    state && now - state.windowStartedAtMs <= OCC_CONTENTION_WARNING_WINDOW_MS
      ? state
      : { count: 0, lastWarnedAtMs: 0, windowStartedAtMs: now };

  currentState.count += 1;
  occContentionWarningState.set(event.table, currentState);

  const canWarn =
    currentState.count >= OCC_CONTENTION_WARNING_THRESHOLD &&
    now - currentState.lastWarnedAtMs >= OCC_CONTENTION_WARNING_DEDUP_MS;
  if (!canWarn) {
    return;
  }

  currentState.lastWarnedAtMs = now;
  logRuntimeEvent("warn", "runtime.occ_contention", {
    message: event.partitioned
      ? `Table "${event.table}" has contention on non-partition-aligned reads; route hot reads through its partition index.`
      : `Table "${event.table}" has contention on table-level reads; add a partition index if writes cluster around one access axis.`,
    table: event.table,
  });
}
