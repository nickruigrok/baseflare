import type {
  AnyFunctionEntry,
  BaseflareFunctionEntry,
  BaseflareManifest,
  BaseflareManifestSource,
  DiscoveredFunctionExport,
} from "./types";

function assertNameSegment(value: string, label: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new Error(`${label} must not be empty`);
  }

  return normalized;
}

function createCanonicalFunctionName(
  modulePath: string,
  exportName: string
): string {
  return `${assertNameSegment(modulePath, "Function modulePath")}:${assertNameSegment(exportName, "Function exportName")}`;
}

function createEntries<TDefinition extends AnyFunctionEntry["definition"]>(
  entries: readonly DiscoveredFunctionExport<TDefinition>[] | undefined,
  seenNames: Set<string>
): readonly BaseflareFunctionEntry<TDefinition>[] | undefined {
  if (!entries || entries.length === 0) {
    return undefined;
  }

  return entries.map(({ definition, exportName, modulePath }) => {
    const name = createCanonicalFunctionName(modulePath, exportName);
    if (seenNames.has(name)) {
      throw new Error(`Duplicate function id "${name}"`);
    }

    seenNames.add(name);
    return { definition, exportName, modulePath, name };
  });
}

export function buildBaseflareManifest(
  source: BaseflareManifestSource
): BaseflareManifest {
  const seenNames = new Set<string>();

  return {
    actionEntries: createEntries(source.actions, seenNames),
    config: source.config,
    http: source.http,
    internalActionEntries: createEntries(source.internalActions, seenNames),
    internalMutationEntries: createEntries(source.internalMutations, seenNames),
    internalQueryEntries: createEntries(source.internalQueries, seenNames),
    mutationEntries: createEntries(source.mutations, seenNames),
    queryEntries: createEntries(source.queries, seenNames),
    rules: source.rules,
    schema: source.schema,
  };
}
