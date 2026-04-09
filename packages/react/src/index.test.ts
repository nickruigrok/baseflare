import { describe, expect, it } from "vitest";

import { reactPackagePlaceholder } from "./index";

describe("@baseflare/react scaffold", () => {
  it("exports a placeholder marker", () => {
    expect(reactPackagePlaceholder).toContain("@baseflare/react");
  });
});
