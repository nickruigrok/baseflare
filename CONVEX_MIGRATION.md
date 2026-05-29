# Convex → Baseflare Migration Guide

This guide covers **every** change required to migrate a codebase from Convex to Baseflare. Baseflare's API is intentionally near-identical to Convex, so most of the migration is mechanical (import path changes). The differences that require real code changes are called out explicitly with **⚠ BEHAVIOR CHANGE** markers.

This document is exhaustive. If you follow it top to bottom, your codebase will run on Baseflare with no surprises. It is written so a coding agent can perform the migration in a single pass.

---

## 0. Migration Overview

A Convex → Baseflare migration has three layers:

1. **Code migration** — import path changes + a handful of API differences (this guide)
2. **Data migration** — export from Convex, remap IDs, import to Baseflare (Section 12)
3. **Infrastructure** — `bf deploy` to your Cloudflare account instead of Convex Cloud (Section 13)

Most apps only need Layer 1 for the code to compile and run. Layers 2 and 3 are for moving live data and going to production.

---

## 1. Directory & File Conventions

| Convex | Baseflare |
|---|---|
| `convex/` directory | `baseflare/` directory (configurable via `functions` in config) |
| `convex.json` | `baseflare.config.ts` (TypeScript, uses `defineConfig()`) |
| `convex/_generated/` | `baseflare/_generated/` |
| `convex/schema.ts` | `baseflare/schema.ts` |
| `convex/auth.config.ts` | `baseflare/auth.ts` (uses `defineAuth()`) |
| `convex/http.ts` | `baseflare/http.ts` (same `httpRouter()` API) |
| `convex/crons.ts` | `baseflare/crons.ts` (uses `defineCrons()`) |

**Action:** Rename the `convex/` directory to `baseflare/`. Update any path references.

---

## 2. Import Path Changes

This is the bulk of the migration. A global find-and-replace handles most of it.

| Convex import | Baseflare import |
|---|---|
| `from 'convex/server'` | `from '@baseflare/server'` |
| `from 'convex/values'` | `from '@baseflare/values'` |
| `from 'convex/react'` | `from '@baseflare/react'` |
| `from 'convex/browser'` | `from '@baseflare/client'` |
| `from './_generated/server'` | `from './_generated/server'` (unchanged) |
| `from './_generated/api'` | `from './_generated/api'` (unchanged) |
| `from './_generated/dataModel'` | `from './_generated/data-model'` (**note: kebab-case**) |
| `from '../convex/_generated/api'` | `from '../baseflare/_generated/api'` |

**Action:** Run these replacements across the entire codebase (both backend `baseflare/` files and frontend files).

---

## 3. Schema Definition

Schema definition is API-identical. Only the import changes.

```typescript
// Before (Convex)
import { defineSchema, defineTable } from 'convex/server'
import { v } from 'convex/values'

// After (Baseflare)
import { defineSchema, defineTable } from '@baseflare/server'
import { v } from '@baseflare/values'
```

`defineSchema()` and `defineTable()` work the same way.

**⚠ BEHAVIOR CHANGE — Indexes:** Convex requires `.withIndex()` at query time to use an index. Baseflare does **not** have `.withIndex()` — SQLite's query planner selects indexes automatically. You still define indexes in the schema the same way (`.index("by_field", ["field"])`), but you remove all `.withIndex()` calls from queries (see Section 5).

---

## 4. Validators (`v.*`)

Mostly identical. Differences:

| Convex | Baseflare | Notes |
|---|---|---|
| `v.string()` | `v.string()` | ✅ same |
| `v.number()` | `v.number()` | ✅ same |
| `v.boolean()` | `v.boolean()` | ✅ same |
| `v.null()` | `v.null()` | ✅ same |
| `v.id("table")` | `v.id("table")` | ✅ same |
| `v.array()` | `v.array()` | ✅ same |
| `v.object()` | `v.object()` | ✅ same |
| `v.record()` | `v.record()` | ✅ same |
| `v.union()` | `v.union()` | ✅ same |
| `v.literal()` | `v.literal()` | ✅ same |
| `v.optional()` | `v.optional()` | ✅ same |
| `v.bytes()` | `v.bytes()` | ✅ same |
| `v.any()` | `v.any()` | ✅ same |
| `v.int64()` | **REMOVED** | ⚠ Use `v.number()`. Baseflare has no `int64`/bigint — JSON storage is float64. |
| `v.float64()` | **REMOVED** | ⚠ Use `v.number()`. Identical to `number` in Baseflare. |

**Validator bounds:** Baseflare uses `.min()` / `.max()` for both value and length bounds:
- Numbers: numeric value bounds.
- Strings: character length bounds.
- Arrays: item count bounds.
- Bytes/vectors: length bounds.

**Action:** Replace `v.int64()` and `v.float64()` with `v.number()`. If you relied on bigint precision (values > 2^53), note that Baseflare does not support bigint in v1 — restructure to store as strings if you need values beyond safe-integer range.

---

## 5. Queries

```typescript
// Before (Convex)
import { query } from './_generated/server'
import { v } from 'convex/values'

export const list = query({
  args: { completed: v.boolean() },
  handler: async (ctx, args) => {
    return await ctx.db
      .query('todos')
      .withIndex('by_completed', /* index equality predicate */)  // ⚠ REMOVE
      .order('desc')
      .collect()
  },
})
```

```typescript
// After (Baseflare)
import { query } from './_generated/server'
import { v } from '@baseflare/values'

export const list = query({
  args: { completed: v.boolean() },
  handler: async (ctx, args) => {
    return await ctx.db
      .query('todos')
      .filter({ completed: args.completed })           // object filter instead of withIndex
      .order('_createdAt', 'desc')                      // ⚠ order takes a FIELD now
      .collect()
  },
})
```

**⚠ BEHAVIOR CHANGE — `.withIndex()` removed:** Remove all named index-selection calls. Replace their equality conditions with object filters such as `.filter({ completed: args.completed })`. SQLite picks the index automatically. You do NOT need to reference the index name in the query — just define it in the schema.

**⚠ BEHAVIOR CHANGE — `.order()` signature:** Convex's `.order('asc' | 'desc')` orders by `_creationTime`. Baseflare's `.order(field, direction)` takes an explicit **field name** plus direction. To replicate Convex's default, use `.order('_createdAt', 'desc')`. You can now order by any field (`.order('priority', 'desc')`), which uses the index if one exists.

**⚠ BEHAVIOR CHANGE — Object filters:** Convex uses expression-builder callbacks. Baseflare deliberately uses serializable object filters with familiar semantics:
- Equality callback on `status` → `{ status: "active" }`
- Greater-than callback on `age` → `{ age: { gt: 18 } }`
- OR callback for active-or-verified → `{ OR: [{ status: "active" }, { verified: true }] }`
- AND callback for active adult users → `{ AND: [{ status: "active" }, { age: { gt: 18 } }] }`
- NOT callback for archived rows → `{ NOT: { archived: true } }`

**Action:** Rewrite all filter predicates from Convex callbacks to Baseflare object filters.

**System field names:**

| Convex | Baseflare |
|---|---|
| `_creationTime` (number, ms) | `_createdAt` (number, ms) |
| `_id` | `_id` (plain UUIDv7 string, not Convex's encoded ID) |

**Action:** Replace all references to `_creationTime` with `_createdAt`.

---

## 6. Mutations

API-identical except imports and system field names.

```typescript
// Database write operations — all identical to Convex:
await ctx.db.insert('todos', { text: 'hello' })       // returns _id
await ctx.db.patch('todos', id, { text: 'updated' })  // shallow merge
await ctx.db.replace('todos', id, { ... })            // full replacement
await ctx.db.delete('todos', id)
await ctx.db.get('todos', id)                          // returns doc or null
```

**⚠ BEHAVIOR CHANGE — `ctx.db.normalizeId()` removed:** Convex's `ctx.db.normalizeId(table, id)` does not exist in Baseflare (our IDs are plain UUIDv7, not table-encoded). To check existence, use `ctx.db.get(table, id)` and check for `null`.

---

## 7. Actions

**⚠ BEHAVIOR CHANGE — Actions can access `ctx.db` directly:** In Convex, actions have NO `ctx.db` — you must call `ctx.runQuery`/`ctx.runMutation`. In Baseflare, actions have direct `ctx.db` access (non-transactional). This means you can simplify code that previously needed a separate mutation just to write one row.

```typescript
// Convex pattern (still works in Baseflare):
export const summarize = action({
  handler: async (ctx, args) => {
    const result = await callLLM(args.text)
    await ctx.runMutation(internal.summaries.save, { result })  // still works
  },
})

// Baseflare can simplify to:
export const summarize = action({
  handler: async (ctx, args) => {
    const result = await callLLM(args.text)
    await ctx.db.insert('summaries', { result })  // direct, non-transactional
  },
})
```

**Important:** `ctx.db` in an action is **non-transactional** — each operation is independent. For atomic multi-step writes, keep using `ctx.runMutation()` to call a mutation. `ctx.runQuery`, `ctx.runMutation`, `ctx.runAction` all still exist and work as in Convex.

**⚠ BEHAVIOR CHANGE — No `"use node"` directive:** Convex has two runtimes (V8 and Node.js) and requires `"use node"` at the top of files using Node APIs. Baseflare runs everything on Cloudflare Workers with `nodejs_compat`. **Remove all `"use node"` directives.** npm packages work without any directive.

---

## 8. Internal Functions

API-identical. Only imports change.

```typescript
// Both Convex and Baseflare:
import { internalQuery, internalMutation, internalAction } from './_generated/server'
import { internal } from './_generated/internal'

export const process = internalMutation({ ... })
// Called via: ctx.runMutation(internal.module.process, args)
```

`internal.*` references work the same way.

---

## 9. HTTP Actions

API-identical. Only imports change.

```typescript
// Before (Convex)
import { httpRouter } from 'convex/server'
import { httpAction } from './_generated/server'

// After (Baseflare)
import { httpRouter, httpAction } from '@baseflare/server'
```

`httpRouter()`, `http.route({ path, method, handler })`, and `http.routeWithPrefix({ pathPrefix, method, handler })` all work identically. `httpAction(async (ctx, request) => Response)` is the same.

---

## 10. Scheduling & Crons

**⚠ BEHAVIOR CHANGE — Scheduler delay limits removed, but cancel works differently:**

| Feature | Convex | Baseflare |
|---|---|---|
| `ctx.scheduler.runAfter(ms, ref, args)` | ✅ returns job ID | ✅ returns job ID, no delay limit (DO Alarms) |
| `ctx.scheduler.runAt(ts, ref, args)` | ✅ returns job ID | ✅ returns job ID, no delay limit |
| `ctx.scheduler.cancel(jobId)` | ✅ | ✅ same |

Scheduling API is effectively identical. Baseflare backs it with a SchedulerDO (Durable Object with Alarms + SQLite) instead of Convex's scheduled functions table, but the developer API is the same.

**Crons:**

```typescript
// Before (Convex)
import { cronJobs } from 'convex/server'
const crons = cronJobs()
crons.interval('cleanup', { hours: 24 }, internal.cleanup.run)
export default crons

// After (Baseflare)
import { defineCrons } from '@baseflare/server'
export default defineCrons({
  cleanup: { schedule: '0 0 * * *', handler: internal.cleanup.run },
})
```

**⚠ BEHAVIOR CHANGE — Cron definition format:** Convex uses `cronJobs()` with `.interval()`/`.daily()`/`.cron()` builder methods. Baseflare uses `defineCrons()` with an object map and standard cron expressions. **Rewrite cron definitions** to the `defineCrons()` object format.

---

## 11. File Storage

| Convex | Baseflare | Notes |
|---|---|---|
| `ctx.storage.generateUploadUrl()` | `ctx.storage.generateUploadUrl()` | ✅ same |
| `ctx.storage.getUrl(id)` | `ctx.storage.getUrl(id)` | ✅ same |
| `ctx.storage.delete(id)` | `ctx.storage.delete(id)` | ✅ same |
| `ctx.storage.store(blob)` | `ctx.storage.store(blob)` | ✅ same (in actions) |
| `ctx.storage.getMetadata(id)` | `ctx.storage.getMetadata(id)` | ✅ same |

File storage API is identical. Backed by R2 instead of Convex storage. Storage IDs differ in format but are opaque strings either way.

---

## 12. Authentication

**⚠ BEHAVIOR CHANGE — Auth system is different:** Convex uses Convex Auth / Clerk / Auth0. Baseflare uses **better-auth** via `defineAuth()`.

```typescript
// baseflare/auth.ts
import { defineAuth } from '@baseflare/server'

export default defineAuth({
  emailAndPassword: { enabled: true },
  socialProviders: {
    google: {
      clientId: process.env.GOOGLE_CLIENT_ID,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET,
    },
  },
})
```

**Migration steps:**
1. Replace Convex auth config with `defineAuth()` (1:1 better-auth config passthrough).
2. Auth routes are served at `/api/auth/*` automatically.
3. `ctx.auth.getUserIdentity()` (Convex) → `ctx.auth` is populated from the session token. Access the user via `ctx.auth` (check exact shape against better-auth session).
4. Frontend: replace Convex auth hooks with `useAuth()` from `@baseflare/react`.

**⚠ This is the most involved part of the migration** — auth user data needs to be migrated (Section 13) and the session/identity shape differs from Convex.

---

## 13. Permissions

**⚠ NEW CAPABILITY — Baseflare has native permissions:** Convex has no built-in permission system; you write authorization logic inside each function. Baseflare adds `defineRules()` (deny-by-default).

```typescript
// baseflare/rules.ts (optional, but recommended)
import { defineRules } from '@baseflare/server'

export default defineRules({
  todos: {
    read: (ctx, row) => row.userId === ctx.auth?.id,
    write: (ctx, row) => row.userId === ctx.auth?.id,
  },
})
```

This is **additive** — you can migrate your existing in-function authorization checks as-is, then optionally move them into `defineRules()` over time. Not required for the migration to work, but recommended for security (deny-by-default).

---

## 14. React / Frontend

| Convex | Baseflare |
|---|---|
| `import { ConvexProvider, ConvexReactClient } from 'convex/react'` | `import { BaseflareProvider, BaseflareClient } from '@baseflare/react'` |
| `useQuery(api.todos.list, args)` | `useQuery(api.todos.list, args)` ✅ same |
| `useMutation(api.todos.create)` | `useMutation(api.todos.create)` ✅ same |
| `useAction(api.todos.run)` | `useAction(api.todos.run)` ✅ same |
| `usePaginatedQuery(...)` | `usePaginatedQuery(...)` ✅ same |
| `useQueries(...)` | `useQueries(...)` ✅ same |
| `preloadQuery(...)` | `preloadQuery(...)` ✅ same |
| `'skip'` arg | `'skip'` arg ✅ same |

```typescript
// Before (Convex)
import { ConvexProvider, ConvexReactClient } from 'convex/react'
const client = new ConvexReactClient(import.meta.env.VITE_CONVEX_URL)

// After (Baseflare)
import { BaseflareProvider, BaseflareClient } from '@baseflare/react'
const client = new BaseflareClient({ url: import.meta.env.BASEFLARE_URL })
```

**⚠ BEHAVIOR CHANGE — Provider name & client constructor:** `ConvexProvider` → `BaseflareProvider`, `ConvexReactClient` → `BaseflareClient`. The client takes an options object `{ url }` instead of a positional URL string.

**Optimistic updates:** Convex's `.withOptimisticUpdate()` pattern is supported on `useMutation`.

---

## 15. Error Handling

**⚠ BEHAVIOR CHANGE — `ConvexError` → `BaseflareError`:**

```typescript
// Before (Convex)
import { ConvexError } from 'convex/values'
throw new ConvexError({ code: 'OUT_OF_STOCK' })

// After (Baseflare) — message-first, data second
import { BaseflareError } from '@baseflare/values'
throw new BaseflareError('Out of stock', { code: 'OUT_OF_STOCK' })
```

**⚠ Signature differs:** `ConvexError(data)` is data-only. `BaseflareError(message, data?)` is message-first with optional structured data. Update all throw sites: the human-readable message comes first, structured payload second. On the client, `error.data` carries the structured payload (same as Convex).

---

## 16. Data Migration (Live Data)

Moving existing data from Convex to Baseflare. This is a one-off script.

**The core problem:** Convex IDs are table-encoded strings. Baseflare IDs are plain UUIDv7. Every document ID and every foreign-key reference (`v.id(...)` fields) must be remapped.

**Steps:**

1. **Export from Convex:**
   ```bash
   npx convex export --path convex-export.zip
   ```
   This produces JSONL files, one per table.

2. **Build an ID mapping:** For every document, generate a new UUIDv7. To preserve creation-time ordering (Baseflare derives `_createdAt` from the UUIDv7 timestamp), craft the UUIDv7 using the original Convex `_creationTime` as the timestamp portion. Build a map: `{ <convexId>: <newUuidv7> }`.

3. **Remap references:** Walk every document. For each field that is a `v.id(...)` reference, replace the old Convex ID with the mapped UUIDv7. Replace the document's own `_id` too. Drop `_creationTime` (Baseflare derives `_createdAt` from the ID).

4. **Import to Baseflare:**
   ```bash
   bf import data.json --env production
   ```
   (Where `data.json` is your remapped data in Baseflare's import format.)

5. **Keep the ID mapping file** — if external systems (webhooks, third-party integrations, analytics) reference old Convex IDs, you need the lookup table.

**Auth data:** User accounts migrate separately — export Convex/Clerk/Auth0 users and import into better-auth's user table. Password hashes may not be portable depending on the source; users may need to reset passwords or you migrate via the original provider's export format.

---

## 17. Deployment

| Convex | Baseflare |
|---|---|
| `npx convex dev` | `bf dev` (local Miniflare) |
| `npx convex deploy` | `bf deploy --env production` |
| Convex Cloud (managed) | Your Cloudflare account |
| `CONVEX_URL` env var | `BASEFLARE_URL` env var |
| `npx convex env set KEY val` | `bf secrets set KEY val --env production` |

**Steps:**
1. `bf login` — OAuth to your Cloudflare account
2. `bf deploy --env production` — provisions Workers, D1, DOs, R2, Vectorize; deploys functions + schema
3. Set `BASEFLARE_URL` in your frontend hosting platform's env vars (points to your deployed Worker)

---

## 18. Feature Parity Checklist

Things in Convex that **do NOT exist** in Baseflare v1 (plan accordingly):

- ❌ `v.int64()` / `v.float64()` / bigint — use `v.number()`
- ❌ `ctx.db.normalizeId()` — use `ctx.db.get()` + null check
- ❌ `.withIndex()` — automatic index selection
- ❌ `"use node"` runtime directive — single runtime
- ❌ `ctx.db.system` system table access — use the dashboard
- ❌ Components system — use npm packages directly
- ❌ Convex's `defineMigration` component — write backfill mutations manually (see plan §1.5)

Things Baseflare has that **Convex does NOT:**

- ✅ Native `defineRules()` permissions (deny-by-default)
- ✅ Actions with direct `ctx.db` access
- ✅ Lifecycle middleware (`defineMiddleware` — onQuery/onMutation/onAction/onSignUp/onSignIn/onSignOut/onSubscription)
- ✅ Self-hostable (your Cloudflare account)
- ✅ `$0-5/month` flat pricing, unlimited seats
- ✅ Order by any field (not just `_creationTime`)

---

## 19. Migration Order (Recommended)

1. Rename `convex/` → `baseflare/`, add `baseflare.config.ts`
2. Global find-replace import paths (Section 2)
3. Replace `_creationTime` → `_createdAt` everywhere
4. Remove `.withIndex()` calls, convert to `.filter()` (Section 5)
5. Update `.order()` calls to take a field name (Section 5)
6. Rewrite filter predicates from Convex callbacks to object filters: equality on `x` → `{ x: y }`
7. Replace `v.int64()`/`v.float64()` → `v.number()`
8. Remove `"use node"` directives
9. Rewrite cron definitions to `defineCrons()` format
10. Migrate auth to `defineAuth()` (better-auth)
11. Replace `ConvexError` → `BaseflareError` (message-first)
12. Update React provider + client constructor
13. Run codegen: `bf generate`
14. `bf dev` — fix any remaining type errors
15. (Production) migrate data (Section 16), `bf deploy`

---

*This guide reflects Baseflare v1. Keep it in sync with the implementation plan as the API evolves.*
