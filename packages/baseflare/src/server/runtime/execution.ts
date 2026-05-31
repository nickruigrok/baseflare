import type {
  ActionCtx,
  ActionDefinition,
  InternalActionDefinition,
  InternalMutationDefinition,
  InternalQueryDefinition,
  MutationCtx,
  MutationDefinition,
  QueryCtx,
  QueryDefinition,
} from "../functions/types";
import type { Rules } from "../permissions/types";
import type { Schema } from "../schema/types";

import { createAuth } from "./auth";
import { D1DatabaseAdapter } from "./d1";
import { coerceValidationError, NotFoundRuntimeError } from "./errors";
import type { FunctionIndex } from "./function-index";
import {
  createMutationDatabaseSession,
  MutationDatabase,
  withMutationRetry,
} from "./mutation-database";
import {
  createSchedulerPlaceholder,
  createStorageActionWriterPlaceholder,
  createStorageReaderPlaceholder,
  createStorageWriterPlaceholder,
} from "./placeholders";
import type { BaseflareExecutionContext, D1Database } from "./types";

interface InvocationOptions {
  readonly database: D1Database;
  readonly executionContext: BaseflareExecutionContext;
  readonly functionIndex: FunctionIndex;
  readonly invocationName?: string;
  readonly requestHeaders: Headers;
  readonly rules?: Rules;
  readonly schema: Schema;
}

function validateArgs<TArgs>(
  definition: { validateArgs(args: unknown): TArgs },
  args: unknown
): TArgs {
  try {
    return definition.validateArgs(args);
  } catch (error) {
    return coerceValidationError(error, "Invalid function arguments");
  }
}

function validateReturn<TResult>(
  definition: { validateReturn(value: unknown): TResult },
  value: unknown
): TResult {
  try {
    return definition.validateReturn(value);
  } catch (error) {
    return coerceValidationError(error, "Invalid function return value");
  }
}

export async function executeQueryDefinition<TResult>(
  definition: InternalQueryDefinition | QueryDefinition,
  options: InvocationOptions & {
    readonly scopedDatabase?: QueryCtx["db"];
  },
  args: unknown
): Promise<TResult> {
  const auth = createAuth(options.requestHeaders);
  const ctx: QueryCtx = {
    auth,
    db:
      options.scopedDatabase ??
      new D1DatabaseAdapter({
        database: options.database,
        getContext: () => ctx,
        rules: options.rules,
        schema: options.schema,
      }),
    storage: createStorageReaderPlaceholder(),
  };

  const validatedArgs = validateArgs(definition, args);
  const result = await definition.handler(ctx, validatedArgs);
  return validateReturn(definition, result) as TResult;
}

export function executeMutationDefinition<TResult>(
  definition: InternalMutationDefinition | MutationDefinition,
  options: InvocationOptions,
  args: unknown
): Promise<TResult> {
  const auth = createAuth(options.requestHeaders);
  const validatedArgs = validateArgs(definition, args);

  return withMutationRetry(
    async () => {
      let ctx!: MutationCtx;
      const mutationDb = new MutationDatabase({
        database: createMutationDatabaseSession(options.database),
        functionName: options.invocationName,
        getContext: () => ctx,
        rules: options.rules,
        schema: options.schema,
      });

      ctx = {
        auth,
        db: mutationDb,
        runQuery(ref, nestedArgs) {
          const entry = options.functionIndex.getByReference("query", ref);
          if (!entry) {
            throw new NotFoundRuntimeError("Unknown query reference");
          }

          return executeQueryDefinition(
            entry.definition as QueryDefinition,
            {
              ...options,
              scopedDatabase: mutationDb,
            },
            nestedArgs
          );
        },
        scheduler: createSchedulerPlaceholder(),
        storage: createStorageWriterPlaceholder(),
      };

      const result = await definition.handler(ctx, validatedArgs);
      const validatedResult = validateReturn(definition, result);
      await mutationDb.commit();

      return validatedResult as TResult;
    },
    3,
    options.invocationName
  );
}

export async function executeActionDefinition<TResult>(
  definition: ActionDefinition | InternalActionDefinition,
  options: InvocationOptions,
  args: unknown
): Promise<TResult> {
  const ctx = createActionContext(options);
  const validatedArgs = validateArgs(definition, args);
  const result = await definition.handler(ctx, validatedArgs);
  return validateReturn(definition, result) as TResult;
}

export function createActionContext(options: InvocationOptions): ActionCtx {
  const auth = createAuth(options.requestHeaders);
  const ctx: ActionCtx = {
    auth,
    db: new D1DatabaseAdapter<ActionCtx>({
      database: options.database,
      getContext: () => ctx,
      rules: options.rules,
      schema: options.schema,
    }),
    runAction(ref, nestedArgs) {
      const entry = options.functionIndex.getByReference("action", ref);
      if (!entry) {
        throw new NotFoundRuntimeError("Unknown action reference");
      }

      return executeActionDefinition(
        entry.definition as ActionDefinition,
        options,
        nestedArgs
      );
    },
    runMutation(ref, nestedArgs) {
      const entry = options.functionIndex.getByReference("mutation", ref);
      if (!entry) {
        throw new NotFoundRuntimeError("Unknown mutation reference");
      }

      return executeMutationDefinition(
        entry.definition as MutationDefinition,
        {
          ...options,
          invocationName: entry.name,
        },
        nestedArgs
      );
    },
    runQuery(ref, nestedArgs) {
      const entry = options.functionIndex.getByReference("query", ref);
      if (!entry) {
        throw new NotFoundRuntimeError("Unknown query reference");
      }

      return executeQueryDefinition(
        entry.definition as QueryDefinition,
        options,
        nestedArgs
      );
    },
    scheduler: createSchedulerPlaceholder(),
    storage: createStorageActionWriterPlaceholder(),
  };

  return ctx;
}
