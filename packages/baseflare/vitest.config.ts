import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const sourcePath = (path: string): string =>
  fileURLToPath(new URL(path, import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "baseflare/client": sourcePath("./src/client/index.ts"),
      "baseflare/server": sourcePath("./src/server/index.ts"),
      "baseflare/values": sourcePath("./src/values/index.ts"),
    },
  },
});
