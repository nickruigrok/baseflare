import { describe, expect, it, vi } from "vitest";

import { cliPackagePlaceholder, runCli } from "./index";

describe("baseflare CLI scaffold", () => {
  it("exports a placeholder marker", () => {
    expect(cliPackagePlaceholder).toContain("Baseflare CLI");
  });

  it("returns a success exit code for placeholder commands", () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

    expect(runCli(["dev"])).toBe(0);
    expect(logSpy).toHaveBeenCalledWith("Requested command: dev");

    logSpy.mockRestore();
  });
});
