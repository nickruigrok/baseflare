import type {
  FunctionKind,
  FunctionReference,
  FunctionVisibility,
} from "../functions/types";

import type {
  AnyFunctionDefinition,
  AnyFunctionEntry,
  BaseflareManifest,
} from "./types";

export interface IndexedFunction {
  readonly definition: AnyFunctionDefinition;
  readonly kind: FunctionKind;
  readonly name: string;
  readonly visibility: FunctionVisibility;
}

function createBucket() {
  return {
    action: new Map<string, IndexedFunction>(),
    mutation: new Map<string, IndexedFunction>(),
    query: new Map<string, IndexedFunction>(),
  };
}

function indexFunctions(
  bucket: ReturnType<typeof createBucket>,
  references: WeakMap<object, IndexedFunction>,
  seenNames: Set<string>,
  entries: readonly AnyFunctionEntry[] | undefined,
  visibility: FunctionVisibility
): void {
  if (!entries) {
    return;
  }

  for (const entry of entries) {
    if (entry.definition.visibility !== visibility) {
      throw new Error(
        `Function "${entry.name}" has visibility "${entry.definition.visibility}" but was indexed as "${visibility}"`
      );
    }

    const indexed: IndexedFunction = {
      definition: entry.definition,
      kind: entry.definition.kind,
      name: entry.name,
      visibility: entry.definition.visibility,
    };

    if (seenNames.has(indexed.name)) {
      throw new Error(`Duplicate function id "${indexed.name}"`);
    }

    seenNames.add(indexed.name);
    bucket[indexed.kind].set(indexed.name, indexed);
    references.set(entry.definition, indexed);
  }
}

export interface FunctionIndex {
  getByName(
    kind: FunctionKind,
    name: string,
    visibility?: FunctionVisibility
  ): IndexedFunction | null;
  getByReference<TArgs, TResult>(
    kind: FunctionKind,
    ref: FunctionReference<TArgs, TResult>
  ): IndexedFunction | null;
}

export function createFunctionIndex(
  manifest: BaseflareManifest
): FunctionIndex {
  const publicFunctions = createBucket();
  const internalFunctions = createBucket();
  const references = new WeakMap<object, IndexedFunction>();
  const seenNames = new Set<string>();

  indexFunctions(
    publicFunctions,
    references,
    seenNames,
    manifest.queryEntries,
    "public"
  );
  indexFunctions(
    publicFunctions,
    references,
    seenNames,
    manifest.mutationEntries,
    "public"
  );
  indexFunctions(
    publicFunctions,
    references,
    seenNames,
    manifest.actionEntries,
    "public"
  );
  indexFunctions(
    internalFunctions,
    references,
    seenNames,
    manifest.internalQueryEntries,
    "internal"
  );
  indexFunctions(
    internalFunctions,
    references,
    seenNames,
    manifest.internalMutationEntries,
    "internal"
  );
  indexFunctions(
    internalFunctions,
    references,
    seenNames,
    manifest.internalActionEntries,
    "internal"
  );

  return {
    getByName(kind, name, visibility = "public") {
      const bucket =
        visibility === "public" ? publicFunctions : internalFunctions;
      return bucket[kind].get(name) ?? null;
    },
    getByReference(kind, ref) {
      if (typeof ref !== "object" || ref === null) {
        return null;
      }

      const indexed = references.get(ref as object);
      if (!indexed || indexed.kind !== kind) {
        return null;
      }

      return indexed;
    },
  };
}
