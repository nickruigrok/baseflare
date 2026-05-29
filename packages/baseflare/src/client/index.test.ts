import { describe, expect, it } from "vitest";

import { clientPackagePlaceholder } from "./index";

describe("baseflare/client scaffold", () => {
  it("exports a placeholder marker", () => {
    expect(clientPackagePlaceholder).toContain("baseflare/client");
  });
});
