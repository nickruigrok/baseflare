import { describe, expect, it } from "vitest";
import { chunkRealtimeDeliveries } from "./shared";
import type { PendingRealtimeDelivery } from "./types";
import {
  REALTIME_DELIVERY_BATCH_MAX_BYTES,
  REALTIME_DELIVERY_BATCH_SIZE,
} from "./types";

function delivery(resultBytes: number): PendingRealtimeDelivery {
  return { resultJson: "x".repeat(resultBytes) } as PendingRealtimeDelivery;
}

describe("chunkRealtimeDeliveries", () => {
  it("chunks small deliveries by item count", () => {
    const deliveries = Array.from(
      { length: REALTIME_DELIVERY_BATCH_SIZE + 1 },
      () => delivery(10)
    );

    const chunks = chunkRealtimeDeliveries(deliveries);

    expect(chunks.map((chunk) => chunk.length)).toEqual([
      REALTIME_DELIVERY_BATCH_SIZE,
      1,
    ]);
  });

  it("flushes on the byte budget before the item count fills", () => {
    const overHalfBudget =
      Math.floor(REALTIME_DELIVERY_BATCH_MAX_BYTES / 2) + 1;
    const deliveries = [
      delivery(overHalfBudget),
      delivery(overHalfBudget),
      delivery(overHalfBudget),
    ];

    const chunks = chunkRealtimeDeliveries(deliveries);

    // Any two of these exceed the byte budget, so each flushes its own chunk
    // long before the 100-item count cap.
    expect(chunks.map((chunk) => chunk.length)).toEqual([1, 1, 1]);
  });

  it("ships a single over-budget delivery alone instead of dropping it", () => {
    const chunks = chunkRealtimeDeliveries([
      delivery(REALTIME_DELIVERY_BATCH_MAX_BYTES + 1),
      delivery(10),
    ]);

    expect(chunks.map((chunk) => chunk.length)).toEqual([1, 1]);
  });

  it("returns no chunks for no deliveries", () => {
    expect(chunkRealtimeDeliveries([])).toEqual([]);
  });
});
