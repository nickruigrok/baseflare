import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    "client/index": "src/client/index.ts",
    "cli/index": "src/cli/index.ts",
    "server/index": "src/server/index.ts",
    "values/index": "src/values/index.ts",
  },
  format: ["esm"],
  dts: { sourcemap: false },
  clean: true,
  define: {
    __BASEFLARE_DEV_WARNINGS__: "false",
  },
  fixedExtension: false,
  minify: "dce-only",
  outputOptions: {
    codeSplitting: true,
  },
  sourcemap: false,
  target: "es2022",
});
