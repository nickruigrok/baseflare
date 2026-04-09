import { describe, expect, it, vi } from "vitest";

import { cliPackagePlaceholder, runCli } from "./index";

describe("baseflare CLI scaffold", () => {
  it("exports a placeholder marker", () => {
    expect(cliPackagePlaceholder).toContain("Baseflare CLI");
  });

  it("returns a success exit code for the scaffold entry point", () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

    expect(runCli(["dev"])).toBe(0);

    logSpy.mockRestore();
  });
});
