import type { BaseflareConfig } from "../config";
import type {
  FunctionKind,
  FunctionReference,
  FunctionVisibility,
} from "../functions/types";
import type { HttpRouter } from "../http/http-router";
import type { Rules } from "../permissions/types";
import type { Schema } from "../schema/types";

type RuntimeFunctionDefinition = FunctionReference<unknown, unknown> & {
  readonly kind: FunctionKind;
  readonly visibility: FunctionVisibility;
  validateArgs(args: unknown): unknown;
  validateReturn(value: unknown): unknown;
};

export type PublicQueryRuntimeDefinition = RuntimeFunctionDefinition & {
  readonly kind: "query";
  readonly visibility: "public";
};

export type PublicMutationRuntimeDefinition = RuntimeFunctionDefinition & {
  readonly kind: "mutation";
  readonly visibility: "public";
};

export type PublicActionRuntimeDefinition = RuntimeFunctionDefinition & {
  readonly kind: "action";
  readonly visibility: "public";
};

export type InternalQueryRuntimeDefinition = RuntimeFunctionDefinition & {
  readonly kind: "query";
  readonly visibility: "internal";
};

export type InternalMutationRuntimeDefinition = RuntimeFunctionDefinition & {
  readonly kind: "mutation";
  readonly visibility: "internal";
};

export type InternalActionRuntimeDefinition = RuntimeFunctionDefinition & {
  readonly kind: "action";
  readonly visibility: "internal";
};

export interface DiscoveredFunctionExport<
  TDefinition extends RuntimeFunctionDefinition = RuntimeFunctionDefinition,
> {
  readonly definition: TDefinition;
  readonly exportName: string;
  readonly modulePath: string;
}

export interface BaseflareFunctionEntry<
  TDefinition extends RuntimeFunctionDefinition = RuntimeFunctionDefinition,
> {
  readonly definition: TDefinition;
  readonly exportName: string;
  readonly modulePath: string;
  readonly name: string;
}

export interface BaseflareManifestSource {
  readonly actions?: readonly DiscoveredFunctionExport<PublicActionRuntimeDefinition>[];
  readonly config?: BaseflareConfig;
  readonly http?: HttpRouter;
  readonly internalActions?: readonly DiscoveredFunctionExport<InternalActionRuntimeDefinition>[];
  readonly internalMutations?: readonly DiscoveredFunctionExport<InternalMutationRuntimeDefinition>[];
  readonly internalQueries?: readonly DiscoveredFunctionExport<InternalQueryRuntimeDefinition>[];
  readonly mutations?: readonly DiscoveredFunctionExport<PublicMutationRuntimeDefinition>[];
  readonly queries?: readonly DiscoveredFunctionExport<PublicQueryRuntimeDefinition>[];
  readonly rules?: Rules;
  readonly schema: Schema;
}

export interface BaseflareManifest {
  readonly actionEntries?: readonly BaseflareFunctionEntry<PublicActionRuntimeDefinition>[];
  readonly config?: BaseflareConfig;
  readonly http?: HttpRouter;
  readonly internalActionEntries?: readonly BaseflareFunctionEntry<InternalActionRuntimeDefinition>[];
  readonly internalMutationEntries?: readonly BaseflareFunctionEntry<InternalMutationRuntimeDefinition>[];
  readonly internalQueryEntries?: readonly BaseflareFunctionEntry<InternalQueryRuntimeDefinition>[];
  readonly mutationEntries?: readonly BaseflareFunctionEntry<PublicMutationRuntimeDefinition>[];
  readonly queryEntries?: readonly BaseflareFunctionEntry<PublicQueryRuntimeDefinition>[];
  readonly rules?: Rules;
  readonly schema: Schema;
}

export type AnyFunctionEntry =
  | BaseflareFunctionEntry<InternalActionRuntimeDefinition>
  | BaseflareFunctionEntry<InternalMutationRuntimeDefinition>
  | BaseflareFunctionEntry<InternalQueryRuntimeDefinition>
  | BaseflareFunctionEntry<PublicActionRuntimeDefinition>
  | BaseflareFunctionEntry<PublicMutationRuntimeDefinition>
  | BaseflareFunctionEntry<PublicQueryRuntimeDefinition>;

export type AnyFunctionDefinition = AnyFunctionEntry["definition"];

export type D1BindingValue =
  | ArrayBuffer
  | ArrayBufferView
  | number
  | null
  | string;

export interface D1Result<TRow = Record<string, unknown>> {
  readonly meta?: {
    readonly changes?: number;
    readonly rows_read?: number;
  } & Record<string, unknown>;
  readonly results?: readonly TRow[];
  readonly success: boolean;
}

export interface D1PreparedStatement {
  all<TRow = Record<string, unknown>>(): Promise<D1Result<TRow>>;
  bind(...values: D1BindingValue[]): D1PreparedStatement;
  first<TRow = Record<string, unknown>>(): Promise<TRow | null>;
  first<TRow extends Record<string, unknown>, K extends keyof TRow>(
    columnName: K
  ): Promise<TRow[K] | null>;
  run(): Promise<D1Result>;
}

export interface D1DatabaseSession {
  batch(
    statements: readonly D1PreparedStatement[]
  ): Promise<readonly D1Result[]>;
  getBookmark(): string | null;
  prepare(query: string): D1PreparedStatement;
}

export interface D1Database {
  batch(
    statements: readonly D1PreparedStatement[]
  ): Promise<readonly D1Result[]>;
  prepare(query: string): D1PreparedStatement;
  withSession?(
    constraint?: "first-primary" | "first-unconstrained" | string
  ): D1DatabaseSession;
}

export interface BaseflareRuntimeEnv {
  APP_DB: D1Database;
}

export interface BaseflareExecutionContext {
  passThroughOnException?(): void;
  waitUntil(promise: Promise<unknown>): void;
}

export interface ExportedHandler<TEnv = BaseflareRuntimeEnv> {
  fetch(
    request: Request,
    env: TEnv,
    ctx: BaseflareExecutionContext
  ): Promise<Response> | Response;
}
