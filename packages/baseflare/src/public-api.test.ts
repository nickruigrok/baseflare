/** biome-ignore-all lint/performance/noNamespaceImport: This test snapshots entire subpath export surfaces. */
import { describe, expect, it } from "vitest";

import * as clientApi from "./client";
import * as runtimeApi from "./runtime";
import * as serverApi from "./server";
import * as valuesApi from "./values";

// Locks the value exports of every published subpath so internal helpers
// cannot leak into the public API unnoticed. Type-only exports are erased at
// runtime and are reviewed through the entrypoint barrels instead.
describe("public API surface", () => {
  it("locks the baseflare/server exports", () => {
    expect(Object.keys(serverApi).sort()).toEqual([
      "HttpRouter",
      "SchemaError",
      "ValidationError",
      "action",
      "defineConfig",
      "defineRules",
      "defineSchema",
      "defineTable",
      "httpAction",
      "httpRouter",
      "internalAction",
      "internalMutation",
      "internalQuery",
      "mutation",
      "query",
    ]);
  });

  it("locks the baseflare/values exports", () => {
    expect(Object.keys(valuesApi).sort()).toEqual([
      "BaseflareError",
      "ErrorCode",
      "SchemaError",
      "ValidationError",
      "generateId",
      "getCreatedAtFromId",
      "getCreatedMsFromId",
      "isUuidV7",
      "maxIdForMs",
      "minIdForMs",
      "paginationOptsValidator",
      "v",
    ]);
  });

  it("locks the baseflare/client exports", () => {
    expect(Object.keys(clientApi).sort()).toEqual(["clientPackagePlaceholder"]);
  });

  it("locks the internal baseflare/runtime exports", () => {
    expect(Object.keys(runtimeApi).sort()).toEqual([
      "RealtimeConnectionDO",
      "RealtimeSubscriptionDO",
      "createWorker",
    ]);
  });
});
