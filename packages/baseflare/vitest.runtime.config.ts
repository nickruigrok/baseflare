import { fileURLToPath } from "node:url";

import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";

const sourcePath = (path: string): string =>
  fileURLToPath(new URL(path, import.meta.url));

export default defineWorkersConfig({
  resolve: {
    alias: {
      "baseflare/client": sourcePath("./src/client/index.ts"),
      "baseflare/server": sourcePath("./src/server/index.ts"),
      "baseflare/values": sourcePath("./src/values/index.ts"),
    },
  },
  test: {
    fileParallelism: false,
    include: ["src/**/*.runtime.test.ts"],
    pool: "@cloudflare/vitest-pool-workers",
    poolOptions: {
      workers: {
        isolatedStorage: false,
        miniflare: {
          compatibilityDate: "2025-12-13",
          compatibilityFlags: ["nodejs_compat"],
          d1Databases: {
            APP_DB: "baseflare-runtime-test-db",
          },
        },
      },
    },
  },
});
