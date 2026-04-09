import { describe, expect, expectTypeOf, it } from "vitest";

import { defineConfig } from "./config";

describe("defineConfig", () => {
  it("preserves typing for valid config objects", () => {
    const config = defineConfig({
      project: "my-app",
      functions: "baseflare",
      external: ["pkg-a"],
      cors: {
        origins: ["https://myapp.com"],
        maxAge: 86_400,
      },
      limits: {
        maxQueryResults: 1000,
        maxUploadSize: "10mb",
      },
      middleware: [],
      worker: {
        compatibilityDate: "2026-04-08",
        compatibilityFlags: ["nodejs_compat"],
      },
    });

    expect(config.project).toBe("my-app");
    expect(config.worker?.compatibilityDate).toBe("2026-04-08");
    expectTypeOf(config.functions).toEqualTypeOf<string | undefined>();
  });

  it("rejects unknown top-level config options", () => {
    expect(() =>
      defineConfig({
        project: "my-app",
        invalid: true,
      } as never)
    ).toThrow(/Unknown config option "invalid"/);
  });
});
