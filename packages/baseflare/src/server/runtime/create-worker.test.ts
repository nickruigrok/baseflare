import { describe, expect, it } from "vitest";

import { PayloadTooLargeRuntimeError } from "./errors";
import { readRequestBodyText } from "./request-body";

describe("worker request body reader", () => {
  it("cancels oversized request body streams", async () => {
    let cancelled = false;
    const request = new Request("http://example.com/api/query/todos:list", {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("too-large"));
        },
        cancel() {
          cancelled = true;
        },
      }),
      duplex: "half",
      method: "POST",
    } as RequestInit);

    await expect(readRequestBodyText(request, 1)).rejects.toBeInstanceOf(
      PayloadTooLargeRuntimeError
    );
    expect(cancelled).toBe(true);
  });
});
