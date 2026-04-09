import { pathToFileURL } from "node:url";

export const cliPackagePlaceholder =
  "TODO: implement the Baseflare CLI commands and orchestration flow" as const;

export function runCli(args: string[] = process.argv.slice(2)): number {
  const commandList = args.length > 0 ? args.join(" ") : "(no command)";

  console.log("Baseflare CLI scaffold is installed.");
  console.log(`Requested command: ${commandList}`);
  console.log(cliPackagePlaceholder);

  return 0;
}

const entrypoint = process.argv[1];

if (entrypoint && import.meta.url === pathToFileURL(entrypoint).href) {
  process.exitCode = runCli();
}
