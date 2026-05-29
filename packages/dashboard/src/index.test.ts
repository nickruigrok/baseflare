import { describe, expect, it } from "vitest";

import { dashboardPackagePlaceholder } from "./index";

describe("baseflare-dashboard scaffold", () => {
  it("exports a placeholder marker", () => {
    expect(dashboardPackagePlaceholder).toContain("baseflare-dashboard");
  });
});
