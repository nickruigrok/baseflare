import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    "client/index": "src/client/index.ts",
    "cli/index": "src/cli/index.ts",
    "server/index": "src/server/index.ts",
    "values/index": "src/values/index.ts",
  },
  format: ["esm"],
  dts: true,
  clean: true,
  target: "es2022",
});
