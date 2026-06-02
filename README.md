# Baseflare

Baseflare is a Cloudflare-native backend framework with a Convex-style developer
experience. It gives you typed validators, schema definitions, query/mutation/
action functions, permissions, and a document database model built on Cloudflare
infrastructure.

The goal is simple: define your data model and server functions in TypeScript,
deploy to Cloudflare, and consume the generated API from your app without
assembling a custom backend stack first.

> [!WARNING]
> Baseflare is early-stage alpha software. The core package now includes
> validators, schema definitions, server function wrappers, permissions, query
> and mutation database APIs, HTTP routing primitives, and the Cloudflare D1
> runtime foundation. The CLI workflow, generated client SDK, React bindings,
> auth, real-time subscriptions, scheduler, storage, and dashboard are still in
> progress.

## Public API Shape

Baseflare is published as one core package with focused subpath imports:

| Import | Purpose | Status |
| --- | --- | --- |
| `baseflare/values` | Validators, shared errors, IDs, pagination, and RPC value types | Implemented |
| `baseflare/server` | Schema, server functions, permissions, HTTP actions, and database interfaces | Implemented |
| `baseflare/client` | Browser/Node client SDK | Placeholder, planned |
| `baseflare` CLI | Project init, dev, deploy, codegen, and environment workflows | Scaffolded, planned |
| `@baseflare/react` | React hooks on top of the client SDK | Package scaffolded, planned |

Internal Cloudflare Worker wiring and runtime adapters are documented in
[IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md). The root README focuses on
the public app-developer API.

## Example Schema

```ts
import { defineSchema, defineTable } from "baseflare/server";
import { v } from "baseflare/values";

export const schema = defineSchema({
  todos: defineTable({
    ownerId: v.string(),
    text: v.string().min(1).max(280),
    completed: v.boolean().default(false),
    tags: v.array(v.string()).default([]),
  }).index("by_owner", ["ownerId"]),
});
```

## Permissions

Rules are deny-by-default. If a table or operation has no rule, access is
denied.

```ts
import { defineRules } from "baseflare/server";

export const rules = defineRules({
  todos: {
    read: async ({ ctx, doc }) =>
      doc.ownerId === (await ctx.auth.getUserIdentity()),
    insert: async ({ ctx, value }) =>
      value.ownerId === (await ctx.auth.getUserIdentity()),
    update: async ({ ctx, existingDoc }) =>
      existingDoc.ownerId === (await ctx.auth.getUserIdentity()),
    delete: async ({ ctx, existingDoc }) =>
      existingDoc.ownerId === (await ctx.auth.getUserIdentity()),
  },
});
```

## Queries

Queries read documents and return typed data. They do not write to the database.

```ts
import { query } from "baseflare/server";
import { v } from "baseflare/values";

export const listTodos = query({
  args: {
    ownerId: v.string(),
  },
  returns: v.array(v.any()),
  async handler(ctx, args) {
    return await ctx.db
      .query("todos")
      .filter({ ownerId: args.ownerId })
      .order("_createdAt", "desc")
      .limit(50)
      .collect();
  },
});
```

Useful query methods include:

- `ctx.db.get(table, id)`
- `ctx.db.query(table).filter(...)`
- `.order(field, "asc" | "desc")`
- `.limit(n)`
- `.first()`
- `.unique()`
- `.take(n)`
- `.count()`
- `.collect()`
- `.paginate(options)`

## Mutations

Mutations are the atomic write primitive. The runtime tracks reads and writes,
detects conflicts, and retries mutation handlers when it can do so safely.
Mutation handlers should be deterministic and retry-safe.

```ts
import { mutation } from "baseflare/server";
import { v } from "baseflare/values";

export const createTodo = mutation({
  args: {
    ownerId: v.string(),
    text: v.string().min(1).max(280),
  },
  returns: v.string(),
  async handler(ctx, args) {
    return await ctx.db.insert("todos", {
      ownerId: args.ownerId,
      text: args.text,
      completed: false,
      tags: [],
    });
  },
});

export const completeTodo = mutation({
  args: {
    id: v.id("todos"),
  },
  async handler(ctx, args) {
    await ctx.db.patch("todos", args.id, {
      completed: true,
    });
  },
});
```

Mutation database methods include:

- `ctx.db.insert(table, doc)`
- `ctx.db.patch(table, id, partial)`
- `ctx.db.replace(table, id, doc)`
- `ctx.db.delete(table, id)`
- the same read methods available in queries

Return value validation is part of the mutation contract. If a mutation returns
an invalid value, its pending writes are not committed.

## Actions

Actions are for side effects: calling APIs, sending email, charging payments,
processing webhooks, and other work that should not be automatically retried as
part of a database transaction.

Actions do not have direct `ctx.db` access. Use `ctx.runQuery()` and
`ctx.runMutation()` for database work. Each `ctx.runMutation()` call is its own
mutation transaction, so atomic multi-write workflows should live in one
mutation.

```ts
import { action } from "baseflare/server";
import { v } from "baseflare/values";

import { createTodo } from "./mutations";

export const importTodo = action({
  args: {
    ownerId: v.string(),
    sourceUrl: v.string(),
  },
  returns: v.string(),
  async handler(ctx, args) {
    const response = await fetch(args.sourceUrl);
    const text = await response.text();

    return await ctx.runMutation(createTodo, {
      ownerId: args.ownerId,
      text,
    });
  },
});
```

## Current Status

Implemented today:

- validators and shared value types
- UUIDv7 document IDs and created-at derivation
- schema definition, schema diffing, and table/index metadata
- query, mutation, action, internal function, and HTTP action wrappers
- document serialization and write validation
- object filters, ordering, pagination cursors, and query builders
- deny-by-default permissions
- D1-backed runtime foundation for queries and mutations

Planned next:

- full CLI workflows for init, local dev, deploy, codegen, and environment
  management
- generated client SDK
- React hooks
- auth helpers
- real-time subscriptions
- scheduler, storage, and vector search adapters
- local dashboard

For the detailed roadmap and runtime architecture, see
[IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md).

## Development

This repository is a monorepo. The main packages are:

- `packages/baseflare` -> the core `baseflare` package with `values`,
  `server`, `client`, and CLI subpaths
- `packages/react` -> `@baseflare/react`
- `packages/dashboard` -> local dashboard package

Install dependencies and run checks:

```bash
pnpm install
pnpm check
pnpm build
```

Useful commands:

```bash
pnpm lint
pnpm typecheck
pnpm test
pnpm format
```

Internal development workflow, linting rules, and commit conventions are
documented in [CONTRIBUTING.md](./CONTRIBUTING.md).

## Contributing

External contributions are not being accepted yet.

The project is still moving quickly, and implementation is currently driven
internally. Public contribution intake will open once the package surfaces,
runtime behavior, and workflow are stable enough to support outside contributors
well.
