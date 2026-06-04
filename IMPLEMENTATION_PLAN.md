# Baseflare — Technical Implementation Plan

## 0. What is Baseflare

### The Problem

Building a modern full-stack app requires stitching together a database, auth, file storage, real-time subscriptions, background jobs, vector search, permissions, and deployment infrastructure. Developers either use a managed BaaS like Convex or Firebase (vendor lock-in, per-seat pricing, single-region), or assemble their own stack from Postgres, Redis, S3, message queues, and deploy pipelines (weeks of infrastructure work before writing any product code).

### The Solution

Baseflare is an open-source, Cloudflare-native Backend-as-a-Service. You define your schema and functions in TypeScript, and Baseflare deploys them as Cloudflare Workers with D1 (database), Durable Objects (real-time + scheduling), R2 (file storage), and Vectorize (vector search). Everything runs on your own Cloudflare account — no intermediary, no vendor lock-in, no per-seat pricing.

The developer experience matches Convex: define a schema, write query/mutation/action functions, import typed hooks in React, and get real-time reactive data out of the box. But instead of running on Convex's managed infrastructure at $25/seat/month, it runs on Cloudflare's global edge network starting at $0/month (free plan) or $5/month flat (paid plan, unlimited seats).

### Who It's For

- **Indie developers and startups** who want Convex-quality DX without the pricing model. Ship a real-time app with auth, permissions, and file storage in a day, pay $0-5/month.
- **Small to mid-size teams** who need full control over their data and infrastructure. Everything runs on your Cloudflare account — you own the data, the deployment, and the billing relationship.
- **Developers already on Cloudflare** who want a structured backend framework instead of wiring together Workers + D1 + R2 + DOs manually.
- **Companies migrating from Convex** who want the same API patterns without the managed service dependency. Baseflare's imports and function signatures are designed for near-drop-in compatibility.

### Why It Exists

Convex proved the model — TypeScript functions with real-time subscriptions, typed client SDKs, and zero infrastructure management is the best developer experience for full-stack apps. But Convex is a managed service with per-seat pricing, single-region deployments, and no self-hosting option. Firebase has similar DX but Google's pricing is unpredictable and the SDK is bloated.

Cloudflare's developer platform (Workers, D1, Durable Objects, R2, Vectorize) is the most complete serverless infrastructure available — global edge deployment, zero cold starts, zero egress fees, generous free tier. But there's no framework that ties it all together into a cohesive BaaS experience. Developers are left wiring bindings, writing boilerplate, and solving the same problems (real-time, auth, permissions, schema management) from scratch.

Baseflare bridges that gap: Convex's developer experience on Cloudflare's infrastructure. Open source, MIT licensed, deploy to your own account.

### Core Philosophy

- **Convex-compatible API surface** — If you know Convex, you know Baseflare. Same `query()`, `mutation()`, `action()` pattern. Same `useQuery()`, `useMutation()` hooks. Same `_generated/api.ts` codegen. Migration is a find-and-replace on import paths.
- **Zero infrastructure abstraction** — No control plane Worker, no system database, no management layer. The CLI talks directly to the Cloudflare API. Your app is just Workers + D1 + R2 + DOs. Nothing between you and the platform.
- **Document model** — Every collection table stores `_id TEXT PRIMARY KEY, _data TEXT NOT NULL, _rev INTEGER NOT NULL DEFAULT 0`. Schema validation at write time. No migrations for field changes. Only table creation and index changes touch D1.
- **Deny-by-default permissions** — Built-in `defineRules()` with no access unless explicitly granted. Convex has no native permission system — developers roll their own. Baseflare has it out of the box.
- **Convex-style action boundaries** — Actions handle side effects and call `ctx.runQuery()` / `ctx.runMutation()` for database work. Mutations remain the atomic database write primitive.
- **No Hono, no wrangler** — Native Workers `fetch()` handler with path-based routing. CLI deploys via CF API directly. Minimal dependency surface.
- **Own your infrastructure** — Everything runs on your Cloudflare account. $0/month on free plan, $5/month flat on paid. No per-seat pricing. No vendor between you and Cloudflare.

### How It Compares

| | Baseflare | Convex | Firebase | Supabase |
|---|---|---|---|---|
| Runtime | CF Workers (edge, global) | Custom V8 (single region) | Google Cloud | AWS (single region) |
| Database | D1 (SQLite) | Custom DB | Firestore | Postgres |
| Real-time | Durable Objects + WebSocket | Built-in sync engine | Firestore listeners | Postgres LISTEN/NOTIFY |
| Pricing | $0-5/month flat | $25/seat/month | Unpredictable | $25/month + usage |
| Self-hosted | Yes (your CF account) | No | No | Yes (complex) |
| Open source | MIT | No | No | Apache 2.0 |
| Auth | better-auth (any provider) | Clerk/Auth0/ConvexAuth | Firebase Auth | GoTrue |
| Permissions | Native deny-by-default | None built-in | Security Rules | RLS (Postgres) |
| File storage | R2 (zero egress) | Built-in | Cloud Storage | S3-compatible |
| Vector search | Vectorize | Built-in | None | pgvector |

### Key Technical Decisions

- **Plain UUIDv7 IDs** — No table encoding, no base32. Standard format, universally parseable. `_createdAt` derived from the UUIDv7 timestamp at read time.
- **Two-column document model** — `_id` + `_data` (JSON). Fields are never touched by migrations. Indexes use `json_extract()`. Orphaned fields clean up naturally on rewrite.
- **SQLite query planner picks indexes** — No `.withIndex()` like Convex. Define indexes in schema, forget about them. SQLite does the rest.
- **Branded `Id<"table">` types** — Codegen produces compile-time safe ID types. You can't pass an `Id<"users">` where `Id<"posts">` is expected.
- **`baseflare.config.ts` for project config** — TypeScript config with `defineConfig()`. CORS, limits, worker settings, custom functions directory. Always strict schema validation — no opt-out.
- **Orphaned table/field handling** — Tables removed from schema become read-only, deletable via dashboard. Fields removed from schema are stripped on rewrite, returned on read until all documents are updated.

**Domain:** baseflare.dev
**License:** MIT

---

## Table of Contents

1. [Architecture Decisions](#1-architecture-decisions)
2. [Cloudflare Resource Mapping](#2-cloudflare-resource-mapping)
3. [Monorepo Setup](#3-monorepo-setup)
4. [Implementation Phases](#4-implementation-phases)
5. [Package Specifications](#5-package-specifications)
6. [Interface Contracts](#6-interface-contracts)
7. [Integration Test Scenarios](#7-integration-test-scenarios)
8. [Coding Conventions](#8-coding-conventions)

---

## 1. Architecture Decisions

### 1.1 Stack

| Layer | Choice | Rationale |
|---|---|---|
| Compute | Cloudflare Workers (`nodejs_compat`) | Serverless, global, auto-scaling, CPU limits enforced by CF plan |
| Database | D1 (SQLite) | Managed, serverless SQL, built-in Time Travel |
| Real-time | Durable Objects (WebSocket) | Hibernation API for efficient connection holding, bidirectional messaging |
| File storage | R2 | S3-compatible, zero egress, global |
| Vector search | Vectorize | CF-native vector database, works alongside D1 |
| Scheduler | SchedulerDO (Alarms API) + Cron Triggers | DO Alarms for delayed execution (no time limit), Cron Triggers for recurring. Full job registry in DO SQLite. |
| Secrets | Worker Secrets | Native CF encryption, no custom encryption layer |
| Local dev | Miniflare (programmatic) | Full CF stack emulation, no wrangler dependency |
| Auth | better-auth v1.6 | Native D1 support, per-request instance pattern |
| Monorepo | Turborepo + pnpm workspaces | Industry standard, reliable dependency hoisting, wide ecosystem support |
| Testing | Vitest + Miniflare | Runtime-agnostic, first-class Cloudflare Workers support via `vitest-pool-workers` |

### 1.2 Worker Configuration

All environment Workers are deployed with these settings (configured via CF API, no wrangler.toml):

```typescript
// CLI sets these on every deploy via the CF Workers API
{
  compatibility_flags: ['nodejs_compat'], // Node.js API support
}
```

**Execution model:**
- **Queries, mutations, and actions** all run as regular Worker invocations. CPU limits are enforced by the user's CF plan (10ms free, 30s paid default, up to 5min paid).
- **Scheduled jobs** (`ctx.scheduler.runAfter()`) use SchedulerDO with Alarms — jobs stored in DO SQLite, executed via DO Alarm wake-ups. No time limit, full job history.
- **Crons** (`defineCrons()`) use native Cron Triggers — Cloudflare invokes the Worker on schedule.

CPU time only counts active processing. Waiting on `fetch()`, D1 queries, R2 reads, or any other I/O does not count. An action that calls an LLM for 30 seconds uses only a few milliseconds of CPU time.

### 1.3 Environment Model

**One Worker per environment.** Complete infrastructure isolation.

```
npx baseflare deploy --env production →
  (first deploy auto-provisions:)
  Worker:    bf-{project}-production
  D1:        bf-{project}-production-db
  R2:        bf-{project}-production-files
  DO:        RealtimeConnectionDO + RealtimeSubscriptionDO + SchedulerDO namespaces (bound to Worker)
  Vectorize: bf-{project}-production-vectors (if vector search enabled)
```

First deploy to a new environment auto-provisions all CF resources. Subsequent deploys update the Worker and schema. `npx baseflare env destroy` deletes all resources.

Environment names are project-scoped slugs and must be unique within a Baseflare project. Baseflare-managed Cloudflare resources keep the `bf-` prefix (`bf-{project}-{env}`) so CLI discovery and destructive operations can safely distinguish them from user-created resources. `baseflare.config.ts` remains committed app configuration. `.baseflare/project.json` is generated local CLI state and stores the Cloudflare resources linked to each environment.

After provisioning, commands resolve `--env <name>` through `.baseflare/project.json` before calling Cloudflare. Registry entries store deterministic resource names plus stable resource IDs where Cloudflare provides them. API calls use IDs when available; names are display values, drift checks, and recovery hints. Cloudflare name lookup is only a strict recovery/linking fallback. If multiple matching resources exist, the CLI fails closed and asks the user to link the environment by explicit resource ID.

```json
{
  "version": 1,
  "project": {
    "slug": "my-app"
  },
  "cloudflare": {
    "accountId": "account-id"
  },
  "environments": {
    "production": {
      "worker": {
        "name": "bf-my-app-production"
      },
      "database": {
        "id": "d1-database-id",
        "name": "bf-my-app-production-db"
      },
      "bucket": {
        "name": "bf-my-app-production-files"
      }
    }
  }
}
```

### 1.4 Management Architecture

No hosted control plane. The CLI (`baseflare`) talks directly to the Cloudflare API for all management operations. The dashboard runs locally.

```bash
npx baseflare login                              # OAuth login via browser
npx baseflare login --profile client-x           # OAuth login for named profile
npx baseflare logout                             # revoke tokens
npx baseflare whoami                             # show current account
npx baseflare dev                                # local dev (Miniflare)
npx baseflare deploy --env staging               # creates env if new → build → deploy → apply indexes
npx baseflare generate                           # regenerate types (_generated/)
npx baseflare env list                           # CF API: list bf-{project}-* resources
npx baseflare env destroy --env staging          # CF API: delete all resources
npx baseflare secrets list --env staging         # list secrets
npx baseflare secrets set KEY val --env staging  # set secret
npx baseflare secrets rm KEY --env staging       # remove secret
npx baseflare backup list --env staging          # D1 Time Travel: list snapshots
npx baseflare backup restore --env staging       # D1 Time Travel: restore snapshot
npx baseflare import data.json --env staging     # bulk import data
npx baseflare export --env staging               # export data as JSON
npx baseflare dashboard                          # starts local Vite dev server
```

Only actual project resources (Workers, D1, R2, DO, Vectorize) are deployed to Cloudflare. No management infrastructure, no system database, no extra Workers.

**Authentication:**

```bash
npx baseflare login     # opens browser → Cloudflare OAuth consent page → authorize → tokens stored
npx baseflare logout    # revokes tokens
npx baseflare whoami    # shows current account + email
```

`npx baseflare login` uses OAuth 2.0 Authorization Code Flow with PKCE — same approach as wrangler. Opens the browser, user authorizes, localhost callback receives tokens. Account ID is fetched automatically from the OAuth token via the CF API. Tokens are stored in `~/.baseflare/credentials.json` (global, not per-project).

For CI/CD: set `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` environment variables. These override OAuth tokens when present.

**Planned bootstrap + local CLI flow (Phase 4):**

Baseflare will use a two-stage CLI model:
1. Bootstrap with the full package name: `npx baseflare new my-app`
2. `new` generates a new project directory and installs the `baseflare` package
3. Day-to-day usage runs the local CLI via package scripts or `npx baseflare`

`new` installs direct app dependencies based on the selected stack:
- Always: `baseflare` (server, values, client subpaths, and CLI)
- React: `@baseflare/react` (which depends on `baseflare/client`)

The CLI will prompt for missing runtime configuration on first local `npx baseflare dev` or `npx baseflare deploy`:

```bash
npx baseflare new my-app
# → scaffolds project
# → installs baseflare in devDependencies
# → installs direct app dependencies based on selected frontend/runtime

cd my-app

npx baseflare dev
# ✗ Not logged in
# → Run `npx baseflare login` first

npx baseflare login
# → Opens browser → Cloudflare OAuth consent page → authorize
# → Tokens stored in ~/.baseflare/credentials.json (default profile)
# → ✓ Logged in as nick@joingrasp.com

npx baseflare login --profile client-x
# → Opens browser → different CF account OAuth → authorize
# → Stored as "client-x" profile
# → ✓ Logged in as admin@clientx.com

npx baseflare dev
# ? Select profile: (only shown if multiple profiles exist)
#   1. default (nick@joingrasp.com)
#   2. client-x (admin@clientx.com)
# → 1
# ? Select account: (only shown if multiple accounts for that profile)
#   1. Nick's Personal (abc123)
#   2. Traece B.V. (def456)
# → 1
# ? Project name: (my-app)              ← defaults to folder name
# → Writes baseflare.config.ts + .env.local
# → Starts Miniflare
```

Second run: local CLI + config are already present, zero prompts unless configuration changes.

**Profiles:**

```json
// ~/.baseflare/credentials.json (global, gitignored by location)
{
  "default": { "accessToken": "...", "refreshToken": "...", "email": "nick@joingrasp.com" },
  "client-x": { "accessToken": "...", "refreshToken": "...", "email": "admin@clientx.com" }
}
```

Project-level configuration is stored in `baseflare.config.ts` (project root, committed). No credentials in the project directory.

```typescript
// baseflare.config.ts (project root, committed)
import { defineConfig } from 'baseflare/server'

export default defineConfig({
  // Required
  project: 'my-app',

  // Functions directory (default: 'baseflare')
  // Supports any relative path — useful for monorepos
  // e.g. 'packages/baseflare/src', 'backend/functions'
  functions: 'baseflare',

  // Packages to exclude from Worker bundling (optional, escape hatch)
  // Use for packages with native bindings or dynamic requires that break when bundled
  external: [],

  // CORS (optional — defaults to allow all origins in dev, none in production)
  cors: {
    origins: ['https://myapp.com', 'http://localhost:5173'],
    maxAge: 86400,
  },

  // Limits (optional — sensible defaults if omitted)
  limits: {
    maxQueryResults: 1000,     // safety net for unbounded .collect()
    maxUploadSize: '10mb',     // file upload cap
  },

  // Middleware (optional — lifecycle hooks for cross-cutting concerns)
  middleware: [],              // e.g. [audit(), softDelete(), encryption()]

  // Worker configuration (optional — passed to CF API on deploy)
  worker: {
    compatibilityDate: '2026-04-08',
    compatibilityFlags: [],    // nodejs_compat always included, user can add extras
  },
})
```

`nodejs_compat` is always injected by the CLI — it's required for `process.env`, npm package support, and better-auth. Users don't need to add it. CPU time limits are never set by Baseflare — Cloudflare enforces based on the user's plan (10ms free, 30s paid, configurable up to 5min on paid). Schema validation is always strict — no opt-out. Use `v.any()` or `v.optional()` for flexible fields.

```
# .env.local (gitignored — auto-managed by CLI)
BASEFLARE_PROFILE=default
CLOUDFLARE_ACCOUNT_ID=abc123
BASEFLARE_URL=http://localhost:4510
```

`npx baseflare dev` writes `BASEFLARE_URL=http://localhost:4510` to `.env.local`. This is the only command that modifies `.env.local` — deploy commands never touch it, avoiding accidental frontend→staging/production pointing.

`npx baseflare deploy` prints the environment URL after deploy:
```bash
npx baseflare deploy --env staging
# ✓ Deployed to https://bf-myapp-staging.your-account.workers.dev
```

The frontend SDK reads the URL from environment:

```typescript
const client = new BaseflareClient({
  url: import.meta.env.BASEFLARE_URL,
})
```

For non-local environments, set `BASEFLARE_URL` in your hosting platform (Vercel env vars, Cloudflare Pages env vars, CI/CD). Each environment (staging, production) gets its own URL configured where the frontend is deployed — not in `.env.local`.

The CLI checks two things before any CF operation: `project` in config and valid credentials (OAuth tokens in `~/.baseflare/credentials.json` or `CLOUDFLARE_API_TOKEN` env var). If not logged in, it prompts `npx baseflare login`.

### 1.5 Deploy Model

```bash
npx baseflare deploy --env production
```

1. CLI bundles the `baseflare/` directory with the Baseflare Worker bundler, aligned with the Rolldown/tsdown toolchain used for package builds
2. CLI wraps the bundle into a Worker entry point template:
   ```typescript
   import { createWorker } from 'baseflare/server'
   import * as userCode from './bundle.js'
   export default createWorker(userCode)
   export { RealtimeConnectionDO, RealtimeSubscriptionDO } from 'baseflare/server'
   ```
3. CLI deploys the Worker via Cloudflare Workers API (`PUT /client/v4/accounts/{id}/workers/scripts/{name}`)
4. CLI applies table/index changes to D1 before traffic reaches the Worker. `createWorker()` does not run DDL during requests.

The Worker IS the code. No dynamic `import()`, no code bundles stored in a database. Cloudflare handles versioning and rollback natively.

**Schema diffing rules:**

Baseflare uses a document model — each row stores fields as a JSON blob. Schema changes to fields (add, remove, rename, type change) don't require database migrations. Only index changes need diffing:

| Change | Classification | Behavior |
|---|---|---|
| Add/remove/rename field | No-op | JSON is flexible, validation is at write time |
| Change field type | No-op | Schema validation catches mismatches at write time |
| Add table | Safe | Auto-applied — `CREATE TABLE` |
| Remove table from schema | Orphan | Table kept as read-only, dashboard shows "Orphaned" badge with row count. User deletes manually via dashboard (D1 API `DROP TABLE`). |
| Add/drop index | Safe | Auto-applied — indexes never lose data |

**Orphaned field handling:** When a field is removed from the schema, old documents still contain it in `_data` JSON. On read, `deserialize()` returns all keys — including fields not in the current schema. The dashboard shows orphaned fields with a count of documents still containing them. When a document is rewritten via `patch` or `replace`, removed fields are stripped from the new version. Fields naturally disappear as documents are updated over time.

**Orphaned table handling:** When a table is removed from the schema but still contains data:
1. `npx baseflare deploy` warns: `⚠ Table "legacy_users" orphaned (142 rows remaining)`
2. Table becomes read-only — runtime rejects inserts/updates/deletes to orphaned tables
3. Dashboard shows orphaned tables with row count and a "Delete table" button
4. User clicks delete → dashboard calls D1 REST API → `DROP TABLE` → gone immediately

Schema validation happens at write time in the Worker, not at the database level.

### 1.6 Local Development

`npx baseflare dev` starts Miniflare programmatically — full Cloudflare stack locally:

```typescript
import { Miniflare } from 'miniflare'

const mf = new Miniflare({
  modules: true,
  script: generatedWorkerEntry,
  d1Databases: { APP_DB: 'bf-dev-db' },
  r2Buckets: { FILES: 'bf-dev-files' },
  durableObjects: {
    REALTIME_CONNECTIONS: 'RealtimeConnectionDO',
    REALTIME_SUBSCRIPTIONS: 'RealtimeSubscriptionDO',
  },
})
```

Hot reload: file watcher detects changes in `baseflare/`, rebuilds the Worker bundle, restarts Miniflare. Codegen runs in the same process.

**Cron emulation:** Miniflare doesn't support automatic cron triggers. `npx baseflare dev` runs its own cron loop — parses `defineCrons()`, uses `cron-parser` to calculate next fire times, and calls `worker.scheduled()` on the Miniflare instance when due. Crons fire locally just like in production. SchedulerDO runs locally via Miniflare.

### 1.7 ID Format

Plain UUIDv7 strings. No encoding layer, no table info in the ID.

```
ID: "019078e5-d29f-7b00-8000-1a2b3c4d5e6f"  ← standard UUIDv7, time-sortable, opaque
```

- **UUIDv7** — timestamp-based, globally unique, time-sortable (ordering by `_id` = chronological)
- **`_createdAt` derived from ID** — UUIDv7's first 48 bits encode milliseconds since Unix epoch. No separate column needed. Computed at read time by `deserialize()`.
- **Plain strings** — every language and library can parse them natively, no custom encoding/decoding
- **No table encoding** — `ctx.db.get('todos', id)` takes an explicit table param, like Convex
- **No `_tables` system table** — nothing needed for ID generation

### 1.8 Application Database (D1 — per-environment)

Baseflare uses a **document model** on top of D1/SQLite. Each collection is a separate table with a small set of framework-managed columns. Schema validation happens at write time in the Worker, not at the database level.

**Table structure (per collection):**

```sql
CREATE TABLE todos (
  _id TEXT PRIMARY KEY,        -- UUIDv7 string (contains timestamp)
  _data TEXT NOT NULL,         -- JSON document: {"text":"hello","completed":false,"orgId":"org1"}
  _rev INTEGER NOT NULL DEFAULT 0 CHECK(_rev >= 0) -- internal row revision
);
```

**Indexes** are SQLite indexes on `json_extract()` expressions, created at deploy time:

```sql
-- From: defineTable({...}).index("by_org", ["orgId"])
CREATE INDEX todos_by_org ON todos (json_extract(_data, '$.orgId'));

-- From: v.string().searchable() on field "title"
-- Standalone FTS5 table, synced by mutation hooks
CREATE VIRTUAL TABLE todos_fts USING fts5(title, _id UNINDEXED);
```

**Internal runtime metadata.** User table and field names cannot start with `_`. Baseflare-owned tables use the reserved `_bf_` prefix. Phase 2 adds dense `_bf_table_versions` rows for table-level mutation conflict detection and sparse `_bf_partition_versions` rows for partition-scoped mutation conflict detection; both are internal and never exposed through documents. Missing partition-version rows mean logical version `0`; missing table-version rows are setup errors.

**Why document model over column-per-field:**
- Add/remove/rename fields → no migration, JSON is flexible
- Change field type → no migration, validation is at write time
- Schema diffing only needs to handle tables and indexes — never field-level changes
- Same model as Convex and other document databases
- The developer API is clean — `ctx.db.get('todos', id)` returns a plain object with `_id`, `_createdAt` (derived from ID), and all fields

**Query execution:** The query builder generates `json_extract()` SQL. SQLite's query planner automatically selects the right index — developers just `.filter()`, never `.withIndex()`. With proper indexes, lookups are just as fast as column-based queries. D1 supports indexes on expressions natively.

The `_` prefix is reserved. Schema validation rejects any developer table starting with `_`.

### 1.9 Vector Search Architecture

D1 doesn't support native vector search. Baseflare uses Cloudflare Vectorize as a sidecar:

```
Developer writes:
  embedding: v.vector({ dimensions: 1536 })

Baseflare:
  - Stores the document in D1 as JSON (all fields except vectors)
  - Stores the vector in Vectorize (with document _id as metadata)
  - .vectorSearch() queries Vectorize → gets _ids → fetches full documents from D1

Schema diffing:
  - Detects v.vector() fields
  - Creates Vectorize index via CF API on first deploy
  - Syncs vectors to Vectorize on insert/update/delete
```

The developer API for vector search:

```typescript
const results = await ctx.db.query('documents')
  .vectorSearch('embedding', queryVector, { limit: 10 })
  .collect()
```

Under the hood it's two calls (Vectorize → D1 JOIN) instead of one (LibSQL vector_top_k), but the developer doesn't see this.

### 1.10 Real-Time Architecture

Durable Objects hold WebSocket connections using the Hibernation API:

```
Client → Worker → RealtimeConnectionDO
                    ├── Holds WebSocket connections
                    ├── Tracks client/session delivery state
                    └── Bridges to RealtimeSubscriptionDO

Subscription flow:
  RealtimeConnectionDO → RealtimeSubscriptionDO
                          ├── Tracks subscriptions + dependencies
                          ├── Re-runs affected queries against D1
                          └── Batches changed results back to connections

Mutation flow:
  Client → Worker → D1 (execute mutation + realtime outbox event)
                  → RealtimeSubscriptionDO.notify(eventId)
                     → Catches up from outbox if notify is missed
                     → Re-runs affected queries
                     → Sends changed results to connection DOs
```

Realtime uses one sharded-capable engine. `N=1` is the simple degenerate
mode; higher shard counts use the same routing and delivery code rather than a
second implementation. `RealtimeConnectionDO` instances shard by client/session
id for even connection spread. `RealtimeSubscriptionDO` instances shard by data
partition so subscribers to the same data are colocated. Cross-partition
queries route to table/global subscription instances so they remain correct.

Partition metadata is a shared table concept, not realtime-only. It can later
support tenant sharding, bulk workflows, observability, and data placement.

### 1.11 Backups

D1 has built-in Time Travel — point-in-time recovery for the last 30 days. No custom backup system needed. The `backup` CLI commands and dashboard backup screen simply wrap D1's Time Travel API.

### 1.12 Known Limitations

- **D1 storage: 10GB per database.** This is a hard Cloudflare limit. Covers 99% of apps. If exceeded, per-tenant sharding (one D1 per org) is the natural path — aligns with how D1 is designed. Not in scope for v1.

---

## 2. Cloudflare Resource Mapping

| Baseflare concept | CF primitive | Notes |
|---|---|---|
| Environment database | D1 database | One per environment |
| File storage | R2 bucket | One per environment |
| Real-time subscriptions | Durable Objects | `RealtimeConnectionDO` for WebSockets + `RealtimeSubscriptionDO` for query evaluation/fanout |
| Scheduled jobs | SchedulerDO (Alarms + SQLite) | Singleton per environment |
| Cron triggers | Cron Trigger | Bound to environment Worker |
| Vector search | Vectorize index | One per environment (if vector fields in schema) |
| Secrets | Worker Secrets | Native CF encryption |
| Environment Worker | Worker script | One per environment |
| Dashboard | Local Vite dev server | `npx baseflare dashboard` |

---

## 3. Monorepo Setup

### 3.1 Package Structure

```
baseflare/
├── packages/
│   ├── baseflare/                  → published package: baseflare
│   │   └── src/
│   │       ├── values/             → baseflare/values (validators + shared types)
│   │       ├── server/             → baseflare/server (schema, functions, db, auth, runtime)
│   │       ├── client/             → baseflare/client (browser/Node SDK)
│   │       └── cli/                → baseflare binary
│   ├── react/                      → @baseflare/react (React hooks)
│   ├── dashboard/                  → baseflare-dashboard (local Vite app, not published)
├── package.json
├── turbo.json
├── tsconfig.base.json
└── LICENSE                         → MIT
```

**Package responsibilities:**

| Package | Responsibility | Runs on |
|---|---|---|
| `baseflare/values` | Subpath export for validators (`v.string()`, `v.number()`, etc.), shared types (error codes, RPC shapes, WebSocket messages), ID utilities (`generateId()`, `getCreatedAtFromId()`) | Everywhere |
| `baseflare/server` | Subpath export for Phase 1 core: `defineConfig()`, `defineSchema()`, `defineTable()`, `query()`, `mutation()`, `action()`, `defineRules()`, HTTP router, query builder, document serialization, schema diffing, write validation, database interfaces. Later phases add auth, crons, middleware, Worker runtime (`createWorker()`), and CF adapters. | CF Worker |
| `baseflare/client` | Subpath export for `BaseflareClient`, WebSocket connection manager, subscription state, optimistic updates, auth methods (`signUp`, `signIn`, `signOut`, `getSession`, `onAuthStateChange`) | Browser/Node |
| `@baseflare/react` | `BaseflareProvider`, `useQuery()`, `useMutation()`, `useAction()`, `useAuth()` | Browser |
| `baseflare-dashboard` | Local Vite React app — data browser, environment management, logs. Dogfoods `@baseflare/react` for data plane, CF API for management. Not published to npm. | Dev machine |
| `baseflare` CLI | Single binary for `new/login/dev/deploy/codegen/generate/env/secrets/backup/dashboard`, codegen engine (analyzes `baseflare/` dir, writes `_generated/`), Worker bundling, CF API client, OAuth client, Miniflare orchestration, cron emulation | Dev machine |

### 3.2 Build Order

```
Level 0: packages/baseflare/src/values
Level 1: packages/baseflare/src/server  (depends on values)
         packages/baseflare/src/client  (depends on values)
         packages/baseflare/src/cli     (uses server/client paths for codegen and templates)
Level 2: @baseflare/react               (depends on baseflare/client)
Level 3: baseflare-dashboard            (depends on baseflare/client, @baseflare/react)
```

The `baseflare` package publishes independent subpath exports for `./values`, `./server`, and `./client`, plus the `baseflare` CLI binary. Importing one subpath must not pull in the other subpaths' runtime code.

Published workspace packages are built with `tsdown`. Codegen writes `_generated/` TypeScript helpers for user projects; it is separate from package bundling. Worker deploy bundling is a later CLI step that consumes app code and generated helpers, and should stay aligned with the Rolldown/tsdown toolchain unless implementation proves lower-level Rolldown control is needed.

### 3.3 Internal Structure of `baseflare/server`

```
packages/baseflare/src/server/
  config.ts       → defineConfig()
  schema/         → defineSchema(), defineTable(), schema diffing
  functions/      → query(), mutation(), action(), internal wrappers
  db/             → query builder (json_extract() SQL), write validation, document serialization, DatabaseReader, DatabaseWriter
  permissions/    → permission engine (defineRules, evaluateRules)
  http/           → httpAction(), httpRouter(), HttpRouter
  runtime/        → Phase 2+: createWorker(), D1 runtime, RealtimeConnectionDO, RealtimeSubscriptionDO, SchedulerDO, R2StorageAdapter, VectorizeAdapter
  auth/           → Phase 5+: defineAuth(), better-auth document adapter, auth manager
```

Pure logic (schema, db, permissions) can be tested with plain `vitest`. Runtime code uses `vitest-pool-workers` with Miniflare.

### 3.4 Developer Imports (Convex-compatible)

```typescript
// baseflare/schema.ts
import { defineSchema, defineTable } from 'baseflare/server'
import { v } from 'baseflare/values'

// baseflare/todos.ts — public functions (callable from client)
import { query, mutation, action } from './_generated/server'
import { v } from 'baseflare/values'
import { api } from './_generated/api'            // reference public functions
import { internal } from './_generated/internal'   // reference internal functions

// baseflare/internal.ts — internal functions (server-only)
import { internalQuery, internalMutation, internalAction } from './_generated/server'

// baseflare/http.ts — custom HTTP endpoints
import { httpRouter } from 'baseflare/server'
import { httpAction } from './_generated/server'
import { internal } from './_generated/internal'

// baseflare/auth.ts
import { defineAuth } from 'baseflare/server'

// baseflare/crons.ts
import { defineCrons } from 'baseflare/server'
import { internal } from './_generated/internal'

// src/App.tsx (frontend)
import { BaseflareProvider, useQuery, useMutation, usePaginatedQuery } from '@baseflare/react'
import { api } from '../baseflare/_generated/api'

// src/server.tsx (SSR)
import { preloadQuery } from '@baseflare/react'
```

**Nested folders as namespaces:**
```
baseflare/
  schema.ts          → schema definition
  auth.ts            → defineAuth()
  http.ts            → httpRouter()
  todos.ts           → api.todos.list, api.todos.create
  billing/
    invoices.ts      → api.billing.invoices.list
    payments.ts      → api.billing.payments.charge
```

**Branded Id types (codegen'd):**
```typescript
// _generated/data-model.ts
type Id<TableName extends string> = string & { __tableName: TableName }

// Developer code — compile-time safety
const userId: Id<"users"> = await ctx.db.insert('users', { name: 'Nick' })
const post = await ctx.db.insert('posts', { authorId: userId })  // ✓
const post2 = await ctx.db.insert('posts', { authorId: postId }) // ✗ TypeScript error
```

---

## 4. Implementation Phases

### Phase 1: Values + Server Core (2 weeks)

**Goal:** Pure TypeScript logic works in isolation. Validators, schema parsing, permissions, query building, ID generation — all testable without Cloudflare.

**Packages:** `baseflare/values`, `baseflare/server` (core logic only, no runtime)

**Deliverables:**
1. `baseflare/values`:
   - Full validator suite: `v.string()`, `v.number()`, `v.boolean()`, `v.bytes()`, `v.null()`, `v.id()`, `v.array()`, `v.object()`, `v.record()`, `v.union()`, `v.literal()`, `v.enum()`, `v.vector()`, `v.any()`, `v.optional()`
   - Validator chains: `.min()`, `.max()`, `.default()`, `.optional()`, `.searchable()`
   - Return value validators (same validators, used in function definitions to validate output)
   - `BaseflareError<T>` — typed application errors with structured data payload
   - Shared types (RPC request/response shapes, WebSocket messages, error codes)
   - Pagination types (`PaginationOptions`, `PaginationResult`, `paginationOptsValidator`)
   - ID utilities (`generateId()`, `getCreatedAtFromId()`)
2. `baseflare/server` (core logic — no CF runtime code yet):
   - Project configuration (`defineConfig`, `BaseflareConfig`)
   - Schema parser (`defineSchema`, `defineTable`)
   - Document serialization/deserialization (plain object ↔ JSON `_data` column, `_createdAt` derived from UUIDv7 in `_id`)
   - Schema diffing (tables + indexes only — field changes are no-ops)
   - Permission engine (`defineRules`, deny-by-default — no access unless explicitly granted, `evaluateRules()` checks context + document)
   - Query builder (`.filter()`, `.order()`, `.limit()`, `.first()`, `.unique()`, `.take(n)`, `.count()`, `.collect()`, `.paginate(opts)` — produces `json_extract()` SQL)
   - Index definition (`.index("name", ["field1", "field2"])` → SQL index on `json_extract()`)
   - Write-time schema validation helpers (`validateInsertData`, `validateReplaceData`, `validatePatchData`) + return value validation
   - Public function wrappers (`query()`, `mutation()`, `action()`)
   - Internal function wrappers (`internalQuery()`, `internalMutation()`, `internalAction()`)
   - Generic HTTP router (`httpRouter()`) and Phase 1 HTTP action wrapper (`httpAction()`) until codegen emits the app-typed helper
   - Database interfaces (`DatabaseReader`, `DatabaseWriter` with `get`, `insert`, `patch`, `replace`, `delete`, `query`)
   - Abstract adapter interfaces (implemented by runtime in Phase 2)

**"Done" criteria:**
```typescript
// Schema parsing → SQL for document tables
const schema = defineSchema({
  todos: defineTable({
    text: v.string(),
    completed: v.boolean().default(false),
    priority: v.union(v.literal("low"), v.literal("medium"), v.literal("high")),
    assignee: v.optional(v.id("users")),
  }).index("by_completed", ["completed"]),
})
const sql = schema.toCreateStatements()
// → ['CREATE TABLE todos (_id TEXT PRIMARY KEY, _data TEXT NOT NULL)']
// → ['CREATE INDEX todos_by_completed ON todos (json_extract(_data, \'$.completed\'))']

// Query builder produces json_extract() SQL
const { sql, params } = createQueryBuilder('todos')
  .filter({ completed: false, status: { in: ['active', 'pending'] } })
  .order('desc')
  .limit(10)
  .toSQL()
// → { sql: 'SELECT _id, _data FROM todos WHERE (...) ORDER BY _id DESC LIMIT ?', params: [0, 'active', 'pending', 10] }

// .unique() throws if 0 or 2+ results
// .take(5) returns first 5 results
// .count() returns SELECT COUNT(*)
// .paginate({ numItems: 10, cursor: null }) returns { page, isDone, continueCursor }

// Internal functions not in generated api object, only in internal object
const myInternal = internalMutation({
  args: { userId: v.id("users") },
  handler: async (ctx, args) => { ... }
})

// Return value validation
const getUser = query({
  args: { id: v.id("users") },
  returns: v.object({ name: v.string(), email: v.string() }),
  handler: async (ctx, args) => { ... }
})

// Typed application errors
throw new BaseflareError({ code: "INSUFFICIENT_FUNDS", balance: 42 })

// HTTP router
const http = httpRouter()
http.route({ path: '/webhooks/stripe', method: 'POST', handler: httpAction(...) })

// ID generation
const id = generateId()
// → '019078e5-d29f-7b00-8000-1a2b3c4d5e6f' (standard UUIDv7, time-sortable)

const createdAt = getCreatedAtFromId(id)
// → Date derived from UUIDv7 timestamp
```

### Phase 2: Worker Runtime + D1 Adapter (3 weeks)

**Goal:** An environment Worker can receive HTTP requests, execute queries and mutations against D1, and enforce permissions.

**Package:** `baseflare/server` (runtime layer added on top of Phase 1 core logic)

**Deliverables:**
1. Internal D1 database adapter — implements `DatabaseReader` and `DatabaseWriter` using `env.APP_DB.prepare()`
   - `ctx.db.get(table, id)` — primary key lookup, deserialize document
   - `ctx.db.insert(table, doc)` — validate, serialize, INSERT, return `_id`
   - `ctx.db.patch(table, id, partial)` — shallow merge, `undefined` removes field, validate result
   - `ctx.db.replace(table, id, doc)` — full replacement, validate, UPDATE entire `_data`
   - `ctx.db.delete(table, id)` — DELETE by `_id`
   - `ctx.db.query(table)` — returns QueryBuilder with `.filter()`, `.order()`, `.limit()`, `.first()`, `.unique()`, `.take(n)`, `.count()`, `.collect()`, `.paginate()`
2. Worker entry point factory (`createWorker()`) — native `fetch()` handler with path-based routing
3. RPC routes: `POST /api/query/:name`, `POST /api/mutation/:name`, `POST /api/action/:name`; bodies must be exact JSON objects shaped as `{ "args": ... }`
4. Internal function routing — `internalQuery`/`internalMutation`/`internalAction` not exposed via RPC, only callable via `ctx.runQuery()`/`ctx.runMutation()`/`ctx.runAction()`
5. Action context — `ActionCtx` with `runQuery`, `runMutation`, `runAction`, `scheduler`, `storage`, `auth`
6. Mutation context — `MutationCtx` with `db`, `auth`, `storage`, `scheduler`, `runQuery`
7. Auth token extraction from headers, `ctx.auth` population
8. Permission enforcement on every operation
9. Collection table/index creation in D1 on deploy (document tables with `_id`, `_data`, `_rev` columns + `json_extract()` indexes); schema application is deploy-owned and never runs inside `createWorker()`
10. Transaction support via `env.APP_DB.batch()`
11. Document serialization/deserialization (plain objects ↔ JSON `_data`)
12. Write-time schema validation + return value validation
13. `BaseflareError<T>` propagation — typed errors surface to client with structured data
14. `createWorker(manifest)` accepts a `BaseflareManifest` with `schema`, optional `config`, discovered function entries, optional `rules`, and optional `http`; canonical function ids are derived from module path + export name and duplicate ids fail during manifest build
15. Runtime-produced RPC failures use a fixed taxonomy: `VALIDATION_ERROR`, `UNAUTHORIZED`, `PERMISSION_DENIED`, `NOT_FOUND`, `MALFORMED_DOCUMENT`, `DATABASE_ERROR`, `NOT_IMPLEMENTED`, `CONFLICT`, `INTERNAL_ERROR`; database details are logged internally and sanitized from client envelopes
16. Mutation-scoped read-your-writes use selective SQL scanning plus `_id`-based overlay reconciliation instead of full-table hydration; broad mutation queries are guarded by internal scan budgets
17. Runtime `.count()` is permission-aware, so it may scan matching rows to enforce read rules; large-table counts should use selective `.filter(...)` clauses and are guarded by internal scan budgets
18. Mutations use a serializable-by-retry concurrency model on D1 with three OCC scopes: point reads/writes track row `_rev`, partition-aligned query reads track `_bf_partition_versions.version`, and broad/unpartitioned query or missing-document reads track `_bf_table_versions.version`
19. Partition metadata is an index attribute: `.index("by_channel", ["channelId"], { partition: true })`. At most one index per table can be partitioned. Single-index tables auto-default that index as the partition axis unless opted out with `{ partition: false }`; adding a second index to an auto-partitioned table requires an explicit partition choice. Composite partition indexes use the full field tuple as the partition value.
20. Reads aligned to the partition index get fine-grained conflict detection. Reads on non-partition indexes, full scans, unindexed filters, cross-partition queries, and unpartitioned tables keep the table-level fallback. Table versions are never removed because they are the correctness floor for every broad read.
21. Partition phantom safety is handled by version bumps on the partition values a document enters or leaves: inserts bump the new partition, deletes bump the old partition, and patches/replaces that move a document bump both old and new partitions. Point reads/writes still use `_rev`; `_rev` is a per-row revision, not a table/partition version.
22. D1 mutation commits gate document writes behind guarded table/partition-version bumps before running document statements, because D1 batch result validation is not a rollback boundary
23. Mutation consistency requires D1 Sessions with `first-primary`; Baseflare-managed deployments provide this, and local tests/custom bindings must implement `withSession("first-primary")` returning a session with `prepare`, `batch`, and `getBookmark`
24. Baseflare does not provide built-in duplicate-execution protection. Side-effectful work belongs in actions, and duplicate handling is application-managed around the specific external system or table that needs it.
25. OCC contention observability emits per-table metrics in every environment: `baseflare.runtime.occ.conflict_retries` and `baseflare.runtime.occ.retry_exhaustions`, tagged with `table`, `partitioned`, `partitionAligned`, and `scope`. Development builds may warn when a table crosses the initial threshold of 10 table-scope retries in 60 seconds without partition-aligned conflict detection; warnings are advisory, deduplicated, and compiled out of production bundles.

Partition indexes should be documented as a data-modeling choice, not an OCC knob: use them when reads and writes cluster around a natural grouping such as messages by channel, tasks by project, or records by tenant. The same partition metadata is a shared table concept for OCC, realtime routing, outbox routing, tenant sharding, bulk workflows, observability, and future data placement work.

**Phase 2 scaling note:** The D1 OCC guard grows with the mutation read/write dependency set. This is correct and production-safe for normal SaaS mutations, but future hardening should monitor SQL size, D1 parameter limits, high-contention retry distributions, and very large bulk-write patterns. Mutation reads currently batch version capture with each data read for correctness; a later performance pass can explore read coalescing/prefetching for handlers with many independent reads without weakening per-read OCC capture. D1 chunk scans use keyset advancement for `_id` and scalar field ordering, with `OFFSET` retained only as a correctness fallback for non-scalar ordered values. Limited mutation queries can over-fetch base rows when many pending inserts sort before the final limit boundary; this is correct because overlay rows are merged and sliced after base reads, but future bulk/performance work can optimize the boundary. Future enterprise work should calibrate contention warning thresholds against hot-partition load tests, add Cloudflare-backed deploy and migration workflows, backup/restore tooling, scan-budget monitoring, and explicit chunked bulk/import APIs instead of making normal mutations more complex.

**"Done" criteria:**
```bash
# Start local dev (Miniflare)
npx baseflare dev

# Call a mutation
curl -X POST http://localhost:4510/api/mutation/todos:create \
  -H "Content-Type: application/json" \
  -d '{"args":{"text":"hello"}}'
# Returns: {"result":{"_id":"3x7kp2mn...","text":"hello","completed":false}}

# Call a query
curl -X POST http://localhost:4510/api/query/todos:list \
  -H "Content-Type: application/json" \
  -d '{"args":{}}'
# Returns: {"result":[{"_id":"...","text":"hello","completed":false}]}

# Permission denied for wrong org
# Returns: {"error":{"code":"PERMISSION_DENIED"}}

```

### Phase 3: Real-Time (Durable Objects) (3 weeks)

**Goal:** Clients subscribe to queries. Mutations push updates to all subscribed clients via Durable Objects.

**Execution model:**

Realtime generalizes the singleton model into one sharded-capable engine.
`N=1` is the simple degenerate mode for small apps and local development.
Higher shard counts use the same routing, registration, invalidation, and
delivery code rather than a second implementation.

Two internal Durable Object roles split the work:

- `RealtimeConnectionDO` holds WebSockets, client/session state, reconnect
  state, and delivery to clients. Connection DOs shard by client/session id for
  even connection spread.
- `RealtimeSubscriptionDO` owns subscription registration, dependency tracking,
  query re-evaluation, and fanout planning. Subscription DOs shard by data
  partition so subscribers to the same data are colocated.

Batched subscription-to-connection delivery bridges the two roles. The client
SDK still presents one WebSocket and normal query subscriptions by default.

Shard count is internal deployment policy, not public API. App developers do
not configure shards in v1. Live shard-count autoscaling/resharding is future
enterprise work. **Open Phase 3 decision:** default production count, `N=1` vs
`N=32`, decided by hibernation/performance tests.

Future managed autoscaling should keep raw shard counts internal. Once shard
generations, reconnect/drain behavior, outbox catch-up, and load metrics exist,
Baseflare can observe realtime DO pressure and increase shard counts
geometrically (`1 -> 2 -> 4 -> 8 -> ...`). Old generations should drain through
client reconnects and registration leases, while new subscription DOs catch up
from `_bf_realtime_outbox`. Downscaling should be conservative and only happen
after sustained low load.

**Subscription tracking:**
- Each subscription is: `{ subscriptionId, clientId, queryName, queryArgs, lastDeliveredVersion, tableDependencies, partitionDependencies }`
- `RealtimeSubscriptionDO` maintains table/partition dependency indexes for affected subscription lookup.
- Query-to-table dependency is tracked at **runtime**, not static analysis. When a query handler executes, `ctx.db` is wrapped with a tracking proxy that records every table accessed (`ctx.db.query("todos")`, `ctx.db.get("users", id)`) into a `Set<string>`. The first execution captures the dependency set, stored with the subscription. This correctly handles helper functions, imported utilities, `ctx.runQuery()`, and any other indirect table access.
- The dependency set is recaptured on every re-evaluation (a query might access different tables or partitions based on args or data). The indexes are updated accordingly.
- Partitioned queries route to data-local subscription DO instances. Cross-partition queries route to table/global subscription DO instances so they never silently go stale. Table/global paths are correct but expensive, so they use stronger debounce and observability.

**Notification pipeline:**
1. Mutation executes on D1 via Worker.
2. The mutation writes compact realtime outbox events in the same D1 commit as data writes.
3. Outbox events include changed tables and every partition value a document enters or leaves. Inserts emit new partition values, deletes emit old values, and patches/replaces emit both old and new values when a partition field changes.
4. Worker sends `notify(eventId)` to affected `RealtimeSubscriptionDO` instances as the fast path.
5. Subscription DOs catch up from the D1 outbox when notifications are missed.
6. Subscription DOs re-run affected queries against D1 with tracking enabled and compare monotonic table/partition versions before re-querying when possible.
7. Changed results are batched per `RealtimeConnectionDO`; connection DOs deliver to clients via WebSocket.

The foundation slice re-evaluates active registrations on notify/catch-up, the dependency-aware invalidation slice narrows that work to registrations whose tracked table/partition dependencies match the outbox event, reconnect restore uses the client's last delivered outbox sequence to trigger catch-up before reporting restore completion, and fanout batching now groups changed results per connection target with item-level delivery acknowledgement while bounding query re-evaluation concurrency. Sharded subscription routing, advanced backpressure queues, periodic reconciliation, and client-side sequence persistence remain later Phase 3 work.

**Recovery model:** Worker-to-subscription notification is recovered by D1 outbox catch-up. Reconnect-triggered recovery is implemented by carrying the last delivered outbox sequence in delivery messages and running subscription DO catch-up during restore. Live periodic reconciliation, connection hibernation behavior, and client SDK sequence persistence remain later Phase 3 work. **Open Phase 3 decision:** live periodic reconciliation interval, balancing worst-case staleness against idle DO hibernation.

**Registration lifecycle:** Connection-to-subscription registrations use leases and epochs. Subscription DOs expire stale registrations and ignore old epochs so restarted/evicted connection DOs do not leave phantom delivery targets.

**Result comparison:** Result hashes may still be used to avoid duplicate pushes after re-evaluation, but reconciliation must compare monotonic table/partition versions first. Hash-only reconciliation would stampede D1 during reconnect storms.

**Fanout limits:** Subscription DOs dedupe in-flight subscription keys, bound D1 re-evaluation concurrency, and batch subscription-to-connection delivery per connection target. Batch acknowledgements are item-level, so undelivered subscription results stay retryable. Table/global subscription paths use stronger debounce and observability because broad realtime queries are correct but expensive. Full backpressure queues remain later Phase 3 work.

**Deliverables:**
1. `RealtimeConnectionDO` Durable Object class:
   - Holds WebSocket connections via Hibernation API
   - Tracks client/session delivery state
   - Registers subscriptions with `RealtimeSubscriptionDO`
   - Reconciles subscriptions on reconnect through outbox-sequence catch-up
   - Periodic checks remain later Phase 3 work
2. `RealtimeSubscriptionDO` Durable Object class:
   - Tracks subscriptions with table/partition dependency indexes
   - Handles outbox catch-up and `notify(eventId)`
   - Re-runs affected queries, re-evaluates permissions, and batches delivery per connection DO
3. Realtime outbox:
   - D1-backed, append-only, compact event rows written with mutation commits
   - Outbox GC based on shard cursors and retained-window fallback
   - Full re-evaluation fallback when a shard cursor falls outside retained history
4. WebSocket endpoint on environment Worker (`GET /api/subscribe`)
5. Client reconnection with subscription restore through connection DO reconciliation
6. Release performance suite:
   - `pnpm test:perf` for deterministic local/simulated realtime scale tests
   - optional `pnpm test:perf:cloudflare` for staging WebSocket/platform stress tests
   - tracks fanout latency, D1 re-evaluations, queue depth, outbox lag, delivery batching, recovery time, and idle DO hibernation

**"Done" criteria:**
```typescript
// Client A subscribes to todos.list
// Client B calls todos.create mutation
// Mutation writes data + realtime outbox event in the same D1 commit
// Worker notifies affected RealtimeSubscriptionDO instances
// Subscription DO re-runs todos.list, detects a changed version/result
// Connection DO delivers the updated list to Client A

// Client A's session is revoked
// Next push re-evaluates permissions, excludes Client A's data

// A document moves from channel A to channel B
// Subscribers to both old and new partitions are invalidated
```

**Phase 3 test plan:**
- Correctness:
  - `N=1` and sharded modes use the same engine behavior
  - partition moves invalidate old and new partition subscribers
  - deletes invalidate old partition subscribers
  - broad queries receive writes from all partitions
- Recovery:
  - missed Worker-to-subscription notify recovers from outbox
  - reconnect restore with a current outbox sequence avoids unnecessary re-evaluation
  - reconnect restore with a stale outbox sequence catches up and delivers current data
  - missed subscription-to-connection delivery recovers through reconnect-triggered catch-up; live periodic reconciliation remains later work
  - expired connection leases and stale epochs stop phantom deliveries
- Performance:
  - compare idle cost/hibernation for `N=1` vs `N=32`
  - verify idle connection/subscription DOs hibernate between reconciliation checks
  - simulate 25k subscriptions distributed across realtime DO instances
  - hot partition evaluates once per subscription key and batches connection delivery
  - broad table/global queries are debounced and bounded by D1 concurrency
  - outbox GC and catch-up stay within configured lag budgets
- Release commands:
  - `pnpm lint`
  - `pnpm typecheck`
  - `pnpm test`
  - `pnpm test:perf`
  - optional before major releases: `pnpm test:perf:cloudflare`

### Phase 4: CLI + Deploy Pipeline (3 weeks)

**Goal:** Full CLI manages environments and deploys code. CLI talks directly to Cloudflare API — no control plane Worker.

**Package:** `baseflare` (single CLI binary: `baseflare`)

**Deliverables:**
1. `baseflare`:
   - `new <name> [--template minimal|backend|todo] [--no-git]` — scaffold a new app in a new directory, install `baseflare`, install direct app dependencies, and initialize Git unless already inside a Git repo or `--no-git` is passed
   - `login` — OAuth 2.0 Authorization Code Flow with PKCE, opens browser, stores tokens in `~/.baseflare/credentials.json`
   - `login --profile <name>` — same flow, stores under named profile for multi-account/multi-email use
   - `logout` — revokes OAuth tokens for current or specified profile
   - `whoami` — shows current profile, account name, email, account ID
   - `dev` — lazy-prompts for profile/account/project name on first run, then starts Miniflare with file watcher and codegen
   - `deploy --env <n>` — lazy-prompts if not configured → create environment if new → Worker bundle → CF Workers API deploy → apply table/index changes to D1
   - `deploy --env <n> --dry-run` — preview table/index changes
   - `generate` — regenerate types
   - `env list` — list environments via CF API (`bf-{project}-*`)
   - `env destroy --env <n>` — delete all CF resources for environment
   - `secrets list --env <n>` — list secrets
   - `secrets set KEY val --env <n>` — set secret via CF Worker Secrets API
   - `secrets rm KEY --env <n>` — remove secret
   - `backup list --env <n>` — list D1 Time Travel snapshots
   - `backup restore --env <n>` — restore D1 Time Travel snapshot
   - `import <file> --env <n>` — bulk import JSON/CSV data into D1 collection tables
   - `export --env <n> [--table <t>]` — export collection data as JSON
   - `dashboard` — starts local Vite dev server
2. Codegen engine — analyzes functions directory (from `baseflare.config.ts` `functions` path, default `baseflare/`, supports nested folders as namespaces), generates:
   - `_generated/api.ts` — typed references to all public functions (`api.module.fn` or `api.folder.module.fn` for nested)
   - `_generated/internal.ts` — typed references to all internal functions (`internal.module.fn`)
   - `_generated/server.ts` — typed `query()`, `mutation()`, `action()`, `httpAction()`, `internalQuery()`, `internalMutation()`, `internalAction()` wrappers
   - `_generated/data-model.ts` — TypeScript types per table from schema, branded `Id<"tableName">` types for compile-time relationship safety
   - `_generated/http.ts` — HTTP router wiring if `baseflare/http.ts` exists
3. OAuth client — PKCE flow, localhost callback server (port 8976), token refresh, named profiles in `~/.baseflare/credentials.json`
4. Lazy config resolver — checks `baseflare.config.ts` + `.env.local`, prompts for missing values (profile, account, project name), writes files on first resolution
4. Cloudflare API client for resource provisioning:
   - Create/delete Workers, D1 databases, R2 buckets, DO namespaces, and Vectorize indexes
   - Store provisioned resource names and IDs in `.baseflare/project.json` after first deploy
   - Resolve `--env <name>` through the project environment registry before Cloudflare calls
   - List environments from the registry, with CF API `bf-{project}-*` discovery as a strict recovery/linking fallback
   - Reject duplicate environment slugs within a project and fail closed on ambiguous Cloudflare name matches

**"Done" criteria:**

Bootstrap flow:
```bash
npx baseflare new my-app
# → creates my-app/
# → installs baseflare
# → installs @baseflare/react only for the todo template

cd my-app
pnpm baseflare dev
```

Login flow:
```bash
npx baseflare login
# → Opens browser to Cloudflare OAuth consent page
# → User authorizes
# → ✓ Logged in as nick@example.com (Account: Traece B.V.)
# → Tokens saved to ~/.baseflare/credentials.json
```

First deploy (prompts for project name, auto-creates environment):
```bash
npx baseflare deploy --env production

? Project name: (my-app)
> my-app

✓ Project name set in baseflare.config.ts
```

First deploy (auto-creates environment):
```bash
npx baseflare deploy --env production
# ✓ Environment 'production' not found — creating...
# ✓ Created Worker: bf-myapp-production
# ✓ Created D1: bf-myapp-production-db
# ✓ Created R2: bf-myapp-production-files
# ✓ Created DO namespaces: RealtimeConnectionDO, RealtimeSubscriptionDO, SchedulerDO
# ✓ Bundle compiled (142kb, 12 functions)
# ✓ Schema applied to D1 (2 tables, 1 index)
# ✓ Worker deployed
# ✓ Deployed to https://bf-myapp-production.your-account.workers.dev
```

Secrets:
```bash
npx baseflare secrets set production STRIPE_KEY sk_live_xxx
# ✓ Secret set on Worker bf-myapp-production
```

Second deploy (environment exists, update only):
```bash
npx baseflare deploy --env production
# ✓ Bundle compiled (142kb, 12 functions)
# ✓ Schema applied to D1 (1 index added)
# ✓ Worker deployed
```

Orphaned table detection:
```bash
npx baseflare deploy --env production
# ✓ Worker deployed
# ✓ Schema applied to D1
# ⚠ Table "old_feature" orphaned (38 rows remaining) — delete via dashboard
```

Dashboard orphaned table management:
```bash
npx baseflare dashboard
# → Shows "old_feature" with red "Orphaned" badge, row count, "Delete table" button
# → User clicks delete → D1 API DROP TABLE → table gone immediately
```

Local dashboard:
```bash
npx baseflare dashboard
# ✓ Dashboard running at http://localhost:4511
```

### Phase 5: Auth (2 weeks)

**Goal:** Users can sign up, sign in, and access is controlled by session tokens. `ctx.auth` is populated in all functions. Auth data uses the same document model as developer data.

**Deliverables:**
1. `defineAuth()` function — developer defines auth config in `baseflare/auth.ts`:
   ```typescript
   // baseflare/auth.ts
   import { defineAuth } from 'baseflare/server'

   export default defineAuth({
     emailAndPassword: { enabled: true },
     socialProviders: {
       google: { clientId: process.env.GOOGLE_CLIENT_ID!, clientSecret: process.env.GOOGLE_CLIENT_SECRET! },
     },
     // All better-auth options pass through 1:1
   })
   ```
2. Custom Baseflare adapter for better-auth v1.6:
   - Uses `createAdapterFactory` from better-auth
   - Stores auth data (user, session, account, verification) in the same document model: `_id TEXT PRIMARY KEY, _data TEXT NOT NULL`
   - Auth tables use same `json_extract()` queries as developer tables
   - Indexes on frequently queried auth fields (email, session token, userId)
   - `supportsJSON: false`, `supportsDates: false`, `supportsBooleans: false` (all stored as JSON text)
   - Auth table names: `_auth_user`, `_auth_session`, `_auth_account`, `_auth_verification`
   - Auth table migrations run on deploy alongside developer table migrations
3. Route mounting at `/api/auth/*` on environment Worker
4. `ctx.auth` population from session token in every query/mutation/action
5. Per-request auth instance (D1 bindings only available in fetch handler)
6. Uses D1 `batch()` for atomicity
7. better-auth provider configuration passed through from `defineAuth()` (Google, GitHub, email/password, etc.)
8. Per-environment auth isolation (separate D1 databases)

**"Done" criteria:**
```bash
# Sign up
curl -X POST https://bf-myapp-production.workers.dev/api/auth/sign-up/email \
  -d '{"email":"test@test.com","password":"secret123"}'
# → creates user document in _auth_user table (same _id, _data format)

TOKEN=$(curl -X POST .../api/auth/sign-in/email \
  -d '{"email":"test@test.com","password":"secret123"}' | jq -r '.token')

curl .../api/query/todos:list -H "Authorization: Bearer $TOKEN"
# → authenticated response with ctx.auth populated
```

### Phase 6: Client + React SDK (2 weeks)

**Goal:** Frontend apps can connect to Baseflare with full type safety, real-time subscriptions, and React hooks.

**Packages:** `baseflare/client`, `@baseflare/react`

**Deliverables:**
1. `baseflare/client`:
   - `BaseflareClient` class (URL config, auth token management)
   - `client.query()`, `client.mutation()`, `client.action()`
   - WebSocket connection manager (auto-reconnect, exponential backoff)
   - Subscription manager (subscribe, unsubscribe, receive updates)
   - Auth methods (`signUp`, `signIn`, `signOut`, `getSession`, `onAuthStateChange`)
   - `BaseflareError<T>` deserialization — typed errors from server propagate to client
2. `@baseflare/react`:
   - `BaseflareProvider` — context provider with client instance
   - `useQuery(api.todos.list, args)` — subscribes, returns reactive data. Pass `'skip'` to conditionally skip.
   - `useMutation(api.todos.create)` — returns callable function with `.withOptimisticUpdate()` chain
   - `useAction(api.todos.summarize)` — returns callable function
   - `usePaginatedQuery(api.todos.list, args, { initialNumItems: 10 })` — cursor-based infinite scroll, reactive, `{ results, status, loadMore, isLoading }`
   - `useQueries({ todos: { query: api.todos.list, args }, users: { query: api.users.list, args } })` — batch multiple subscriptions
   - `preloadQuery(api.todos.list, args)` + `usePreloadedQuery(preloaded)` — SSR preloading
   - `useAuth()` — auth state + methods
   - Optimistic updates with rollback
   - Loading/error states

**"Done" criteria:**
```tsx
const client = new BaseflareClient({
  url: 'https://bf-myapp-production.workers.dev',
})

function TodoList() {
  const todos = useQuery(api.todos.list, { completed: false })
  const createTodo = useMutation(api.todos.create)
  // Real-time updates via DO-backed WebSocket
}

function InfiniteList() {
  const { results, status, loadMore } = usePaginatedQuery(
    api.todos.list, {}, { initialNumItems: 10 }
  )
  // → results grows as user scrolls, status = 'CanLoadMore' | 'Exhausted'
  // → loadMore(10) fetches next page
}

// SSR
const preloaded = await preloadQuery(api.todos.list, { completed: false })
// → pass to component, usePreloadedQuery(preloaded) returns data immediately
```

### Phase 7: Platform Features (5 weeks)

#### 7a: File Storage (R2) (1 week)

**Deliverables:**
1. R2 storage provider — implements `StorageReader`, `StorageWriter`, `StorageActionWriter` using `env.FILES`
2. `ctx.storage.generateUploadUrl()` — returns signed URL for client-side direct upload to R2 (used in mutations)
3. `ctx.storage.store(blob)` — server-side upload from actions (returns storage ID)
4. `ctx.storage.getUrl(id)` — returns public/signed URL for a stored file
5. `ctx.storage.getMetadata(id)` — returns file size, content type, upload timestamp
6. `ctx.storage.delete(id)` — deletes R2 object
7. File metadata stored as R2 custom metadata — no D1 table needed
8. Upload route on environment Worker for client-side uploads via signed URL

**"Done" criteria:**
```typescript
// In a mutation — generate upload URL for client
const uploadUrl = await ctx.storage.generateUploadUrl()
// → Client POSTs file to this URL, gets back storageId

// In an action — server-side upload
const storageId = await ctx.storage.store(blob)

// Get URL and metadata
const url = await ctx.storage.getUrl(storageId)
const meta = await ctx.storage.getMetadata(storageId)
// → { size: 1024, contentType: 'application/pdf', uploadedAt: Date }

// Delete
await ctx.storage.delete(storageId)

// Client-side upload flow:
// 1. Client calls mutation → mutation returns uploadUrl
// 2. Client POSTs file to uploadUrl → gets storageId
// 3. Client calls mutation with storageId → mutation stores reference in document
```

#### 7b: Scheduler + Cron (2 weeks)

**Implementation: SchedulerDO with Alarms for scheduled functions, CF Cron Triggers for crons**

All scheduled jobs are owned by a `SchedulerDO` Durable Object. Jobs are stored in the DO's internal SQLite — not in the app's D1. The DO uses the Alarms API for wake-ups, which has no time limit (can schedule weeks or months ahead). CF Cron Triggers handle recurring jobs.

**SchedulerDO internal SQLite schema:**
```sql
CREATE TABLE jobs (
  id TEXT PRIMARY KEY,           -- UUIDv7
  function_ref TEXT NOT NULL,    -- e.g. 'internal.emails.send'
  args TEXT NOT NULL,            -- JSON-serialized arguments
  scheduled_at INTEGER NOT NULL, -- timestamp: when runAfter/runAt was called
  execute_at INTEGER NOT NULL,   -- timestamp: when the job should execute
  status TEXT NOT NULL DEFAULT 'pending',  -- pending | running | succeeded | failed | canceled
  error TEXT,                    -- error message if failed
  completed_at INTEGER           -- timestamp: when job finished
);
```

**Execution model:**
1. `ctx.scheduler.runAfter(delayMs, ref, args)` → Worker sends job to SchedulerDO via stub
2. SchedulerDO inserts job into its SQLite with status `pending`
3. DO recalculates the next alarm: `SELECT MIN(execute_at) FROM jobs WHERE status = 'pending'`
4. DO sets alarm for that timestamp (DO Alarms API — no time limit)
5. Alarm fires → DO queries all jobs where `execute_at <= now AND status = 'pending'`
6. For each due job: set status `running`, call the environment Worker's function endpoint
7. Worker executes the function, returns success/failure
8. DO updates job status to `succeeded` or `failed` (with error message)
9. DO recalculates next alarm for remaining pending jobs

**Cancel:** `ctx.scheduler.cancel(jobId)` → Worker tells SchedulerDO → status set to `canceled`, alarm recalculated.

**Retries:** Optional per-job config. If a job fails and has retries remaining, DO re-inserts with exponential backoff delay and decremented retry count.


**Deliverables:**
1. `SchedulerDO` Durable Object class (SQLite-backed):
   - Job CRUD in internal SQLite (insert, cancel, query status)
   - Alarm management (recalculate next wake-up after every job change)
   - Alarm handler: execute all due jobs, report results, recalculate
   - Job execution: calls environment Worker function endpoint with job args
   - Job history: queryable by dashboard (list pending, running, succeeded, failed)
2. `ctx.scheduler.runAfter(delayMs, ref, args)` — sends job to SchedulerDO, returns job ID
3. `ctx.scheduler.runAt(timestamp, ref, args)` — same, absolute timestamp. No time limit.
4. `ctx.scheduler.cancel(jobId)` — sends cancel to SchedulerDO, status set to `canceled`
5. `defineCrons()` → CF Cron Triggers configured via Workers API on deploy
6. Cron handler on environment Worker — triggered by CF `scheduled` event, executes function

**"Done" criteria:**
```typescript
// Schedule with delay — returns job ID
const jobId = await ctx.scheduler.runAfter(5000, internal.emails.send, { to: 'test@test.com' })
// → Job stored in SchedulerDO SQLite, alarm set for now + 5s

// Schedule far in the future — no time limit
await ctx.scheduler.runAt(Date.now() + 30 * 86400000, internal.reports.generate, {})
// → Job stored, alarm set for 30 days from now

// Cancel a scheduled job
await ctx.scheduler.cancel(jobId)
// → Status set to 'canceled' in SchedulerDO, alarm recalculated

// Job executes → status updated to 'succeeded' with completed_at timestamp
// Job fails → status updated to 'failed' with error message

// Dashboard queries SchedulerDO for job list:
// → [{ id, functionRef, status: 'succeeded', scheduledAt, completedAt }]
// → [{ id, functionRef, status: 'failed', error: 'Connection timeout', scheduledAt }]
// → [{ id, functionRef, status: 'pending', executeAt: '2026-05-08T...' }]

// Cron defined in crons.ts → CF Cron Trigger created on deploy
```

#### 7c: Vector Search (Vectorize) (1 week)

**Deliverables:**
1. `v.vector({ dimensions })` validator — detected by schema differ
2. On deploy: create/update Vectorize index via CF API (if vector fields detected)
3. Vector fields are NOT stored in `_data` JSON (too large) — only in Vectorize with the document `_id` as metadata
4. Mutation hooks: on insert/update/delete, sync vector fields to Vectorize separately from the D1 document
5. `.vectorSearch('field', vector, opts)` on query builder:
   - Query Vectorize for nearest neighbors → get row IDs
   - Query D1 with those row IDs → return full documents (deserialized from `_data`)
6. Metadata filtering in Vectorize for pre-filtering

**"Done" criteria:**
```typescript
const schema = defineSchema({
  documents: defineTable({
    title: v.string(),
    embedding: v.vector({ dimensions: 1536 }),
  }),
})
await ctx.db.insert('documents', { title: 'test', embedding: vectorData })
// → D1: _data = {"title":"test"} (no embedding in JSON)
// → Vectorize: vector stored with _id as metadata

const results = await ctx.db.query('documents')
  .vectorSearch('embedding', queryVector, { limit: 10 })
  .collect()
// → Vectorize query → get IDs → D1 lookup → full documents with title
```

#### 7d: Full-Text Search (FTS5) (1 week)

**Deliverables:**
1. `v.string().searchable()` → FTS5 virtual table in D1
2. Schema differ generates FTS5 DDL on deploy
3. Mutation hooks sync searchable fields from `_data` JSON into FTS5 table on insert/update/delete
4. `.search('field', query)` on query builder → FTS5 MATCH query joined back to source table

**"Done" criteria:**
```typescript
const results = await ctx.db.query('posts')
  .search('title', 'typescript reactive')
  .collect()
// → FTS5 MATCH query → join with posts table → full documents
```

#### 7e: Middleware (1 week)

**Deliverables:**
1. `defineMiddleware()` — lifecycle hooks for cross-cutting concerns (audit, soft delete, encryption, rate limiting)
2. Middleware pipeline in Worker request handler — executed in registration order, each hook can modify context/data or throw to abort

**Lifecycle hooks:**

| Hook | Timing | Arguments | Use Cases |
|---|---|---|---|
| `onQuery.before` | Before query execution | `(ctx, { queryName, args })` | Add filters (soft delete, tenant isolation), rate limiting |
| `onQuery.after` | After query execution | `(ctx, { queryName, args, result })` | Transform results (decryption, enrichment) |
| `onMutation.before` | Before mutation execution | `(ctx, { table, operation, data, id })` | Validate data, transform fields, cancel via `{ preventDefault: true }` |
| `onMutation.after` | After mutation commits | `(ctx, { table, operation, data, result, functionName })` | Audit logging, webhooks, cache invalidation |
| `onAction.before` | Before action execution | `(ctx, { actionName, args })` | Rate limiting, feature flags |
| `onAction.after` | After action execution | `(ctx, { actionName, args, result })` | Logging, metrics |
| `onSignUp.before` | Before user creation | `(ctx, { email, provider })` | Block disposable emails, enforce invite codes, rate limit. `{ preventDefault: true }` to block. |
| `onSignUp.after` | After user created | `(ctx, { userId, email, provider })` | Welcome email, create default data, analytics |
| `onSignIn.before` | Before authentication | `(ctx, { email, provider })` | IP blocking, check if account suspended. `{ preventDefault: true }` to block. |
| `onSignIn.after` | After authenticated | `(ctx, { userId, email, provider })` | Last login timestamp, session tracking |
| `onSignOut.before` | Before sign out | `(ctx, { userId })` | Pre-logout cleanup |
| `onSignOut.after` | After sign out | `(ctx, { userId })` | Cleanup, session logging |
| `onSubscription.create` | When subscription registered | `(ctx, { queryName, args, clientId })` | Tracking, connection limits |
| `onSubscription.push` | Before WebSocket push | `(ctx, { queryName, data, clientId })` | Transform data, add metadata |

**Middleware API:**

```typescript
// baseflare/middleware.ts
import { defineMiddleware } from 'baseflare/server'

export const audit = defineMiddleware({
  name: 'audit',

  onMutation: {
    after: async (ctx, { table, operation, functionName }) => {
      await ctx.db.insert('audit_log', {
        table,
        operation,
        userId: ctx.auth?.id,
        functionName,
        timestamp: new Date().toISOString(),
      })
    },
  },
})

export const softDelete = defineMiddleware({
  name: 'soft-delete',

  onQuery: {
    before: (ctx, { queryName, args }) => {
      // Automatically filter out soft-deleted records
      return { filter: { deletedAt: null } }
    },
  },

  onMutation: {
    before: async (ctx, { table, operation, id }) => {
      if (operation === 'delete') {
        await ctx.db.patch(table, id, { deletedAt: new Date().toISOString() })
        return { preventDefault: true }  // cancel the real delete
      }
    },
  },
})
```

**Registration in config:**

```typescript
// baseflare.config.ts
import { defineConfig } from 'baseflare/server'
import { audit, softDelete } from './baseflare/middleware'

export default defineConfig({
  project: 'my-app',
  middleware: [audit(), softDelete()],
})
```

### Phase 8: Dashboard (4 weeks)

**Goal:** Full local dashboard for inspecting and managing environments.

**Package:** `baseflare-dashboard`

**Tech:** React + Vite + TanStack Router + shadcn/ui

**Architecture:** Runs locally via `npx baseflare dashboard`. Two data sources:
- Data plane screens use `@baseflare/react` with `useQuery()`/`useMutation()` to connect to the selected environment Worker (dogfooding)
- Management screens use CF API directly (via CLI's CF client) for env settings, secrets, deploys

**Screens:**
1. Environment picker + connection setup
2. Data browser (table viewer, filtering, inline editing)
3. Function explorer (list, execute, signatures)
4. Schema viewer (tables, fields, indexes, relationships)
   - Orphaned tables: red "Orphaned" badge, row count, "Delete table" button (D1 API `DROP TABLE`)
   - Orphaned fields: shows per-table count of documents still containing removed fields
5. Scheduler dashboard:
   - Crons: list of CF Cron Triggers with schedule, last run, next run
   - Jobs: queryable list from SchedulerDO — pending, running, succeeded, failed with timestamps, function ref, args, error messages. Filterable by status.
6. Live subscriptions viewer
7. Function profiler (p50/p95/p99, rows read/written)
8. Logs viewer (real-time stream, filtering)
9. Backups (D1 Time Travel — list restore points, restore)

**"Done" criteria:** `npx baseflare dashboard` starts local Vite server, all screens functional against a running environment.

### Phase 9: Testing Harness (future — post-v1)

**Package:** `baseflare/test`

Deferred to post-v1. Includes `createTestCtx()`, mock utilities, subscription tracking, and Miniflare-backed test environments. Internal integration tests will use `vitest` + `vitest-pool-workers` directly during v1 development.

---

## 5. Package Specifications

### 5.0 baseflare

**Purpose:** The single published core package. It exposes `baseflare/values`, `baseflare/server`, and `baseflare/client` as independent subpath exports, plus the `baseflare` CLI binary. There is no broad root API; users import the subpath that matches their runtime surface.

```json
{
  "name": "baseflare",
  "bin": {
    "baseflare": "./bin/baseflare.js"
  },
  "exports": {
    "./values": "./dist/values/index.js",
    "./server": "./dist/server/index.js",
    "./client": "./dist/client/index.js"
  }
}
```

### 5.0.1 baseflare/values

**Purpose:** Validators, shared types, ID utilities, typed errors. The minimal shared leaf — imported by both server and client. Uses `uuidv7` (Apache-2.0, zero transitive dependencies) for UUIDv7 generation.

```typescript
// Validators (Convex-compatible API)
export const v: {
  // Primitives
  string(): StringValidator;      // .min(), .max(), .default(), .optional(), .searchable()
  number(): NumberValidator;      // .min(), .max(), .default(), .optional()
  boolean(): BooleanValidator;    // .default(), .optional()
  bytes(): BytesValidator;        // Uint8Array, .optional()
  null(): NullValidator;          // null literal

  // References (codegen produces branded Id<"tableName"> types for compile-time safety)
  id(table: string): IdValidator;  // runtime: validates UUIDv7 format. types: Id<"users"> ≠ Id<"posts")

  // Composites
  array(inner: Validator): ArrayValidator;
  object(shape: Record<string, Validator>): ObjectValidator;
  record(values: Validator): RecordValidator;  // Record<string, T>

  // Union + literals
  union(...members: Validator[]): UnionValidator;   // v.union(v.string(), v.null())
  literal(value: string | number | boolean): LiteralValidator;  // v.literal("active")

  // Special
  enum(values: [string, ...string[]]): EnumValidator;  // shorthand for union of string literals
  vector(opts: { dimensions: number }): VectorValidator;
  any(): AnyValidator;
  optional(inner: Validator): OptionalValidator;    // makes any validator optional
}

// Typed application errors (propagate structured data to client)
export class BaseflareError<T = undefined> extends Error {
  constructor(
    public readonly data: T,
    message?: string,
  )
}

// RPC types
export interface QueryRequest { name: string; args: Record<string, unknown> }
export interface MutationRequest { name: string; args: Record<string, unknown> }
export interface ActionRequest { name: string; args: Record<string, unknown> }
export interface RPCResponse<T> { result: T }
export interface RPCError { code: string; message: string; data?: unknown }

// WebSocket types
export interface WSSubscribeMessage { type: 'subscribe'; query: string; args: Record<string, unknown>; subscriptionId: string }
export interface WSUnsubscribeMessage { type: 'unsubscribe'; subscriptionId: string }
export interface WSResultEvent { type: 'result'; subscriptionId: string; data: unknown }
export interface WSErrorEvent { type: 'error'; subscriptionId: string; message: string }
export interface WSHeartbeatEvent { type: 'heartbeat'; timestamp: number }

// Errors
export const ErrorCode: {
  Unauthorized: 'UNAUTHORIZED'
  PermissionDenied: 'PERMISSION_DENIED'
  NotFound: 'NOT_FOUND'
  ValidationError: 'VALIDATION_ERROR'
  SchemaError: 'SCHEMA_ERROR'
  DeployError: 'DEPLOY_ERROR'
  InternalError: 'INTERNAL_ERROR'
}
export type ErrorCode = typeof ErrorCode[keyof typeof ErrorCode]

// Pagination
export interface PaginationOptions { numItems: number; cursor: string | null }
export interface PaginationResult<T> { page: T[]; isDone: boolean; continueCursor: string }
export const paginationOptsValidator: Validator  // reusable validator for pagination args

// ID utilities
export function generateId(): string  // UUIDv7 string
export function getCreatedAtFromId(id: string): Date  // extracts timestamp from UUIDv7
```

### 5.0.2 baseflare/server

**Purpose:** Everything server-side — schema API, function wrappers, database, permissions, auth, HTTP actions, and CF Worker runtime. Depends on `baseflare/values`.

```typescript
// Project configuration
export function defineConfig(config: BaseflareConfig): BaseflareConfig

// Schema (developer-facing API)
export function defineSchema(tables: Record<string, TableDef>): Schema
export function defineTable(fields: Record<string, FieldDef>): TableBuilder

// Public functions (callable from client)
export function query(def: { args: Validators; returns?: Validator; handler: (ctx: QueryCtx, args) => T }): QueryDef
export function mutation(def: { args: Validators; returns?: Validator; handler: (ctx: MutationCtx, args) => T }): MutationDef
export function action(def: { args: Validators; returns?: Validator; handler: (ctx: ActionCtx, args) => T }): ActionDef

// Internal functions (server-only, not callable from client)
export function internalQuery(def): InternalQueryDef
export function internalMutation(def): InternalMutationDef
export function internalAction(def): InternalActionDef

// HTTP actions (generic Phase 1/bootstrap helper; app code uses the generated helper)
export function httpAction(handler: (ctx: ActionCtx, request: Request) => Promise<Response>): HttpAction
export function httpRouter(): HttpRouter

// HttpRouter — exact match first, then prefix match (same as Convex)
class HttpRouter {
  route(config: { path: string; method: string; handler: HttpAction }): void
  routeWithPrefix(config: { pathPrefix: string; method: string; handler: HttpAction }): void
  lookup(method: string, path: string): HttpActionHandler | null  // internal
}

// Rules
export function defineRules(rules): Rules  // deny-by-default: no access unless explicitly granted

// Write validation helpers
export function validateInsertData(table: TableDefinition, data: Record<string, unknown>): Record<string, unknown>
export function validateReplaceData(table: TableDefinition, data: Record<string, unknown>): Record<string, unknown>
export function validatePatchData(
  table: TableDefinition,
  current: Record<string, unknown>,
  patch: Record<string, unknown>,
): Record<string, unknown>

// QueryCtx (available in queries and mutations)
interface QueryCtx {
  db: DatabaseReader;
  auth: Auth;                     // ctx.auth.getUserIdentity()
  storage: StorageReader;         // ctx.storage.getUrl(id)
}

// MutationCtx (available in mutations)
interface MutationCtx extends QueryCtx {
  db: DatabaseWriter;
  storage: StorageWriter;         // ctx.storage.generateUploadUrl(), .delete()
  scheduler: Scheduler;           // ctx.scheduler.runAfter(), .runAt()
  runQuery(ref, args): Promise<T>;  // call a query within same transaction
}

// ActionCtx (available in actions)
interface ActionCtx {
  auth: Auth;
  storage: StorageActionWriter;   // ctx.storage.store(blob), .getUrl(), .delete()
  scheduler: Scheduler;
  runQuery(ref, args): Promise<T>;
  runMutation(ref, args): Promise<T>;
  runAction(ref, args): Promise<T>;
}

// DatabaseReader
interface DatabaseReader {
  get(table: string, id: string): Promise<Doc | null>;
  query(table: string): QueryBuilder;
}

// DatabaseWriter extends DatabaseReader
interface DatabaseWriter extends DatabaseReader {
  insert(table: string, doc: Record<string, unknown>): Promise<string>;  // returns _id
  patch(table: string, id: string, partial: Record<string, unknown>): Promise<void>;  // shallow merge, undefined removes field
  replace(table: string, id: string, doc: Record<string, unknown>): Promise<void>;  // full replacement
  delete(table: string, id: string): Promise<void>;
}

export function createQueryBuilder(table: string): QueryBuilder

// QueryBuilder
interface QueryBuilder {
  filter(filter: FilterObject): QueryBuilder;
  order(direction: 'asc' | 'desc'): QueryBuilder;
  order(field: string, direction: 'asc' | 'desc'): QueryBuilder;
  limit(n: number): QueryBuilder;
  collect(): Promise<Doc[]>;
  first(): Promise<Doc | null>;
  unique(): Promise<Doc>;          // throws if 0 or 2+ results
  take(n: number): Promise<Doc[]>; // shorthand for .limit(n).collect()
  count(): Promise<number>;        // permission-aware runtime count
  paginate(opts: PaginationOptions): Promise<PaginationResult<Doc>>;
}

// Scheduler (backed by SchedulerDO with Alarms — no time limit, full job history)
interface Scheduler {
  runAfter(delayMs: number, ref: FunctionRef, args: any): Promise<string>;  // returns job ID
  runAt(timestamp: number, ref: FunctionRef, args: any): Promise<string>;   // returns job ID, no time limit
}

// Storage
interface StorageReader {
  getUrl(id: string): Promise<string | null>;
}

// Later-phase additions (not part of the Phase 1-complete surface)
export function defineAuth(config): AuthConfig
export function defineCrons(crons): Crons
export function defineMiddleware(def): Middleware
interface StorageWriter extends StorageReader {
  generateUploadUrl(): Promise<string>;  // signed URL for client-side upload
  delete(id: string): Promise<void>;
}
interface StorageActionWriter extends StorageWriter {
  store(blob: Blob): Promise<string>;  // server-side upload, returns storage ID
}

// Document serialization
export function serialize(doc: Record<string, unknown>): { _data: string }
export function deserialize(row: { _id: string; _data: string }): Record<string, unknown>

// Schema diffing + validation (internal)
export function diffSchemas(current: Schema, target: Schema): SchemaDiff
export function validateInsertData(table: TableDefinition, data: Record<string, unknown>): Record<string, unknown>
export function validateReplaceData(table: TableDefinition, data: Record<string, unknown>): Record<string, unknown>
export function validatePatchData(
  table: TableDefinition,
  current: Record<string, unknown>,
  patch: Record<string, unknown>,
): Record<string, unknown>

// Worker entry point factory
export function createWorker(userCode: UserCodeBundle): ExportedHandler

// Durable Objects
export class RealtimeConnectionDO implements DurableObject { ... }
export class RealtimeSubscriptionDO implements DurableObject { ... }
```

**Key design note:** Actions do not have direct `ctx.db` access. Use `ctx.runQuery()` and `ctx.runMutation()` for database work from actions. Each `ctx.runMutation()` call is its own mutation transaction, so atomic multi-write workflows should live in one mutation.

**Worker bindings (configured via CF API on deploy, no wrangler.toml):**
```typescript
{
  d1_databases: [{ binding: 'APP_DB', id: '...' }],
  r2_buckets: [{ binding: 'FILES', name: '...' }],
  durable_objects: { bindings: [
    { name: 'REALTIME_CONNECTIONS', class_name: 'RealtimeConnectionDO' },
    { name: 'REALTIME_SUBSCRIPTIONS', class_name: 'RealtimeSubscriptionDO' },
    { name: 'SCHEDULER', class_name: 'SchedulerDO' },
  ] },
  vectorize: [{ binding: 'VECTORS', index_name: '...' }],
}
```

### 5.0.3 baseflare/client

**Purpose:** Browser/Node SDK. Connects to a Baseflare environment Worker via HTTP + WebSocket.

```typescript
export class BaseflareClient {
  constructor(config: { url: string });

  query<T>(ref: FunctionReference<'query'>, args?: any): Promise<T>;
  mutation<T>(ref: FunctionReference<'mutation'>, args?: any): Promise<T>;
  action<T>(ref: FunctionReference<'action'>, args?: any): Promise<T>;

  subscribe<T>(ref: FunctionReference<'query'>, args: any, callback: (data: T) => void): Unsubscribe;

  auth: {
    signUp(opts: { email: string; password: string; name?: string }): Promise<AuthResult>;
    signIn(opts: { email: string; password: string }): Promise<AuthResult>;
    signOut(): Promise<void>;
    getSession(): Promise<Session | null>;
    onAuthStateChange(callback: (session: Session | null) => void): Unsubscribe;
  };
}
```

**Internal:** WebSocket connection manager with auto-reconnect and exponential backoff. Subscription state tracked client-side. Auth token stored and sent with every request.

### 5.1 @baseflare/react

**Purpose:** React hooks wrapping the client SDK.

```typescript
export function BaseflareProvider(props: { client: BaseflareClient; children: ReactNode }): JSX.Element;

// Core hooks
export function useQuery<T>(ref: FunctionReference<'query'>, args?: any | 'skip'): T | undefined;
export function useMutation<T>(ref: FunctionReference<'mutation'>): ReactMutation<T>;
export function useAction<T>(ref: FunctionReference<'action'>): (args: any) => Promise<T>;

// Pagination
export function usePaginatedQuery<T>(
  ref: FunctionReference<'query'>,
  args: any,
  opts: { initialNumItems: number },
): { results: T[]; status: 'LoadingFirstPage' | 'CanLoadMore' | 'Exhausted'; loadMore: (n: number) => void; isLoading: boolean };

// Batch queries (for dashboards, parallel data loading)
export function useQueries(queries: Record<string, { query: FunctionReference<'query'>; args: any }>): Record<string, any>;

// SSR support
export function preloadQuery<T>(ref: FunctionReference<'query'>, args?: any): Promise<Preloaded<T>>;
export function usePreloadedQuery<T>(preloaded: Preloaded<T>): T;

// Auth
export function useAuth(): {
  isAuthenticated: boolean;
  isLoading: boolean;
  user: User | null;
  signUp: (opts: SignUpOpts) => Promise<AuthResult>;
  signIn: (opts: SignInOpts) => Promise<AuthResult>;
  signOut: () => Promise<void>;
};
```

**Behavior:**
- `useQuery` subscribes on mount, unsubscribes on unmount. Returns `undefined` while loading, then reactive data. Pass `'skip'` as args to conditionally skip the query.
- `useMutation` returns a callable function with `.withOptimisticUpdate()` chain for optimistic updates via a store API.
- `useAction` returns a callable function for side-effect operations.
- `usePaginatedQuery` manages cursor-based pagination, concatenates pages, supports `loadMore(n)` for infinite scroll. Fully reactive.
- `useQueries` batches multiple query subscriptions into one hook call. Useful for dashboards with many data sources.
- `preloadQuery` / `usePreloadedQuery` enables SSR — preload data server-side, hydrate on client without loading flash.

### 5.4 baseflare/test (future — post-v1)

Deferred. Will provide `createTestCtx()`, mock utilities, and subscription tracking for developer-facing testing. See Phase 9.

---

## 6. Interface Contracts

### 6.1 Server Internals: Pure Logic ↔ Runtime

The Worker creates internal adapter instances from CF bindings and passes them to core logic. These adapters are runtime plumbing, not public APIs:

```typescript
// Inside Worker request handler
const db = new D1DatabaseAdapter({
  database: env.APP_DB,
  getContext: () => ctx,
  schema,
  rules,
})
const storage = new R2StorageAdapter(env.FILES)
const vectors = new VectorizeAdapter(env.VECTORS)

// Core query builder produces json_extract() SQL
const querySQL = createQueryBuilder('todos').filter({ orgId }).toSQL()
const result = await db.execute(querySQL.sql, querySQL.params)

// Core deserializes JSON _data into plain objects, derives _createdAt from _id
const docs = result.rows.map(row => deserialize(row))
// → [{ _id: "...", _createdAt: 1709000000, text: "hello", orgId: "org1", completed: false }]

// Permission engine works on plain objects
const filtered = docs.filter(doc =>
  evaluateRules(rules, { tableName: 'todos', operation: 'read', ctx, doc })
)

// On mutation: core validates document against schema before writing
const validated = validateInsertData(schema.tables.todos, {
  text: 'hello',
  completed: false,
})
const serialized = serialize(validated)
await db.execute('INSERT INTO todos (_id, _data) VALUES (?, ?)',
  [id, serialized._data])
```

### 6.2 Server ↔ Durable Object

```typescript
// Worker notifies DO after mutation
const doId = env.SUBSCRIPTIONS.idFromName('default')
const doStub = env.SUBSCRIPTIONS.get(doId)
await doStub.fetch('http://internal/notify', {
  method: 'POST',
  body: JSON.stringify({ tablesChanged: ['todos'] }),
})

// DO has its own D1 binding (configured in Worker bindings, same database)
// DO re-runs affected queries using its own env.APP_DB — no binding serialization
```

### 6.3 CLI ↔ Cloudflare API

```typescript
// CLI resolves credentials in order:
// 1. CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID env vars (CI/CD)
// 2. BASEFLARE_PROFILE from .env.local → lookup in ~/.baseflare/credentials.json + CLOUDFLARE_ACCOUNT_ID from .env.local
const cf = new CloudflareClient({ credentials: loadCredentials() })

// Create environment
const d1 = await cf.d1.create({ name: `bf-${project}-${envName}-db` })
const r2 = await cf.r2.create({ name: `bf-${project}-${envName}-files` })
const worker = await cf.workers.deploy({ name: `bf-${project}-${envName}`, script, bindings })
await saveEnvironmentResources(envName, {
  databaseId: d1.id,
  bucketName: r2.name,
  workerName: worker.name,
})

// Deploy code
await cf.workers.deploy({ name: workerName, script: bundledWorker, bindings })
await cf.d1.execute(d1Id, migrationSQL)
```

### 6.4 Client ↔ Server (Wire Protocol)

All RPC requests are JSON over HTTP. Real-time uses WebSocket.

```
Request headers (all data plane requests):
  Content-Type: application/json
  Authorization: Bearer <session-token>

Response format (success):
  { "result": <T> }

Response format (error):
  { "error": { "code": "PERMISSION_DENIED", "message": "..." } }

WebSocket (real-time):
  Client connects: GET /api/subscribe → 101 Upgrade → WebSocket

  Client sends:  { "type": "subscribe", "query": "todos:list", "args": {...}, "subscriptionId": "sub_123" }
  Client sends:  { "type": "unsubscribe", "subscriptionId": "sub_123" }
  Server sends:  { "type": "result", "subscriptionId": "sub_123", "data": [...] }
  Server sends:  { "type": "heartbeat", "timestamp": 1709000000 }
```

---

## 7. Integration Test Scenarios

These tests span multiple packages and define end-to-end correctness. They must all pass before any phase is considered complete.

### 7.1 Basic CRUD Flow
1. Define schema with `todos` collection
2. Insert a document
3. Query it back — matches what was inserted
4. Patch a field
5. Query again — reflects the patch
6. Delete it
7. Query again — returns empty

### 7.2 Permission Enforcement
1. Define rules: read/write only within own org
2. User A (org1) inserts a document
3. User B (org2) queries — gets empty results (document silently excluded)
4. User B tries to patch User A's document — PermissionDenied
5. User B tries to delete User A's document — PermissionDenied
6. User A queries — sees the document

### 7.3 Real-Time Subscription
1. Client A subscribes to `todos.list` via WebSocket
2. Client B creates a todo via mutation
3. Client A receives WebSocket message with updated list within 500ms
4. Client B deletes the todo
5. Client A receives WebSocket message with empty list

### 7.4 Schema Evolution
1. Deploy with schema v1 (todos: text, completed)
2. Insert some documents
3. Deploy schema v2 (adds priority field)
4. Insert new document with priority
5. Query — old documents have no priority field (document model), new document has it
6. No migration needed, no downtime
7. Deploy schema v3 (removes completed field) — old documents still return `completed` on read
8. Patch an old document — `completed` field stripped from rewritten document
9. Deploy schema v4 (removes todos table entirely) — deploy warns `⚠ Table "todos" orphaned`
10. Runtime rejects `ctx.db.insert('todos', ...)` — table is read-only
11. Dashboard shows "Orphaned" badge with row count, "Delete table" button works

### 7.5 Deploy Pipeline
1. Run `npx baseflare deploy --env staging` (first time)
2. Verify CF resources created (Worker, D1, R2, DO namespaces)
3. Call todos.create via HTTP — works
4. Deploy updated code with new "archive" function
5. Call todos.archive via HTTP — works
6. Old functions still work

### 7.6 Auth Flow
1. Sign up via `/api/auth/sign-up/email`
2. Sign in via `/api/auth/sign-in/email` — get token
3. Call query with token — `ctx.auth` populated
4. Call query without token — `ctx.auth` is null
5. Permission rule denies unauthenticated access

### 7.7 Application-Managed Duplicate Handling
1. Side-effectful work runs in actions, which are not retried by the runtime
2. Apps that need duplicate handling store their own operation keys/results in app tables or the external system they call

### 7.8 Environment Isolation
1. Deploy to "staging" and "production" environments
2. Insert document in staging
3. Query production — empty (separate D1 database)
4. Query staging — document exists

### 7.9 Cloudflare Resource Provisioning
1. `deploy --env staging` (first time) → verify Worker, D1, R2, DO namespaces created via CF API
2. `deploy --env staging` (second time) → verify existing resources updated, not recreated
3. `env destroy --env staging` → verify all resources deleted
4. `secrets set KEY val --env staging` → verify Worker Secret set
5. `deploy --env staging` → verify Worker deployed, D1 collection tables and indexes created

### 7.10 Durable Object Subscriptions
1. Client A connects to DO via WebSocket
2. DO tracks subscription
3. Mutation fires on Worker, Worker notifies DO
4. DO re-evaluates query against D1, pushes update to Client A
5. Client A disconnects, DO cleans up
6. Client A reconnects — resubscribes automatically

### 7.11 Vectorize Integration
1. Insert row with vector field → vector synced to Vectorize
2. `.vectorSearch()` → queries Vectorize → JOINs with D1
3. Delete row → vector removed from Vectorize
4. Update vector field → Vectorize updated

### 7.12 Scheduled Functions (SchedulerDO)
1. `ctx.scheduler.runAfter(5000, ...)` → job stored in SchedulerDO SQLite, alarm set
2. Alarm fires after 5s → SchedulerDO calls Worker function endpoint → executes
3. Job status updated to `succeeded` with `completed_at` timestamp
4. `ctx.scheduler.runAt(Date.now() + 7 days, ...)` → job stored, alarm set 7 days out (no time limit)
5. `ctx.scheduler.cancel(jobId)` → status set to `canceled`, alarm recalculated, job does not execute
6. Job fails → status set to `failed` with error message stored
7. Dashboard queries SchedulerDO → returns job list with status, timestamps, errors
8. Cron trigger fires → Worker receives `scheduled` event, runs function

### 7.13 Internal Functions
1. Define `internalMutation` that writes to DB
2. Call from action via `ctx.runMutation(internal.module.fn, args)` — works
3. Call same function from client via RPC — rejected (not in `api` object)
4. Codegen produces `_generated/internal.ts` with typed references

### 7.14 Mutation Database Write Operations
1. `ctx.db.insert('todos', { text: 'hello' })` → returns `_id`, document in D1
2. `ctx.db.patch('todos', id, { text: 'updated' })` → shallow merge, field updated
3. `ctx.db.patch('todos', id, { tag: undefined })` → field removed from document
4. `ctx.db.replace('todos', id, { text: 'replaced' })` → full replacement, old fields gone
5. `ctx.db.delete('todos', id)` → document removed

### 7.15 Query Builder Advanced
1. `.unique()` with exactly 1 result → returns document
2. `.unique()` with 0 results → throws
3. `.unique()` with 2+ results → throws
4. `.take(5)` → returns first 5 documents
5. `.count()` → returns permission-aware count; use `.filter(...)` for large tables
6. `.paginate({ numItems: 2, cursor: null })` → returns `{ page: [...], isDone: false, continueCursor: '...' }`
7. `.paginate({ numItems: 2, cursor: prevCursor })` → returns next page

### 7.16 HTTP Actions
1. Define `httpRouter` in `baseflare/http.ts` with POST `/webhooks/test` route
2. Deploy, POST to `/webhooks/test` → handler executes, returns custom Response
3. Handler calls `ctx.runMutation(internal.webhooks.process, body)` — works
4. GET to unregistered path → 404
5. Prefix matching: `routeWithPrefix('/api/v1/', ...)` matches `/api/v1/users`

### 7.17 Action Behavior
1. Action handler accesses `ctx.runQuery(api.todos.list)` — works
2. Action handler accesses `ctx.runMutation(internal.todos.create, args)` — works
3. Action handler accesses `ctx.db.query('todos')` — TypeScript error
4. Action handler writes multiple documents by calling one mutation that performs the atomic workflow

### 7.18 Return Value Validation
1. Query with `returns: v.string()` returns a string — passes
2. Query with `returns: v.string()` returns a number — throws validation error
3. Return validator mismatch surfaces as clear error in logs

### 7.19 Typed Errors
1. Mutation throws `new BaseflareError({ code: 'OUT_OF_STOCK', remaining: 0 })`
2. Client receives error with structured `data` payload: `{ code: 'OUT_OF_STOCK', remaining: 0 }`
3. Regular `throw new Error('...')` surfaces as generic error without data

---

## 8. Coding Conventions

### 8.1 File Naming

- **All source files:** kebab-case (`query-builder.ts`, `schema-differ.ts`)
- **Test files:** `{module}.test.ts` co-located next to source (`query-builder.test.ts`)
- **Index files:** `index.ts` for public API re-exports only

### 8.2 Code Style

- No semicolons
- Single quotes
- 2-space indentation
- Explicit return types on all exported functions
- No `any` in public APIs (use `unknown` + type guards)
- Prefer `interface` over `type` for object shapes
- Prefer `const` over `let`
- No classes unless state management requires it (prefer functions)
- Error handling: throw typed errors from `baseflare/values`

### 8.3 Import Order

```typescript
// 1. External packages
import { betterAuth } from 'better-auth'

// 2. Internal package subpaths
import { ErrorCode } from 'baseflare/values'

// 3. Relative imports
import { createQueryBuilder } from './query-builder'
```

### 8.4 Error Handling

All user-facing errors use the `BaseflareError<T>` class from `baseflare/values`:

```typescript
import { BaseflareError } from 'baseflare/values'

// Developer throws typed errors in functions:
throw new BaseflareError({ code: 'OUT_OF_STOCK', remaining: 0 })

// Client receives structured data:
try {
  await createTodo(args)
} catch (e) {
  if (e instanceof BaseflareError) {
    console.log(e.data)  // { code: 'OUT_OF_STOCK', remaining: 0 }
  }
}
```

Internal system errors (permission denied, validation, schema errors) use `ErrorCode` enum and the standard `RPCError` wire format. `BaseflareError<T>` is strictly for application-level errors thrown by developer code.

### 8.5 Testing

- Every exported function has at least one test
- Tests are co-located: `src/db/query-builder.ts` → `src/db/query-builder.test.ts`
- Use `describe`/`it` blocks with clear names
- No mocking unless testing external boundaries (HTTP, CF bindings)
- Unit tests (`baseflare/server` pure logic): plain `vitest`, no Miniflare
- Integration tests (`baseflare/server` runtime): `vitest-pool-workers` (runs tests inside Workers runtime with Miniflare)
- E2E tests: full CLI → Miniflare flow
- Integration tests go in `tests/integration/` at the package root

### 8.6 Git

- Conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`
- Branch per phase: `phase-1/core`, `phase-2/worker`
- PR per package change — never mix packages in one PR
- All tests must pass before merge

### 8.7 Cloudflare-Specific

- Worker bindings are always typed via `Env` interface
- Never import `miniflare` in production code — only in dev/test
- D1 queries always use parameterized statements (never string interpolation)
- Durable Object state is always serialized as JSON (no binary)
- R2 keys use the format `{fileId}` (flat namespace per environment bucket)

### 8.8 Documentation

- Every package has a `README.md` with: purpose, installation, API overview
- Every exported function has a JSDoc comment
- No inline comments unless explaining non-obvious logic

---

## Appendix: Decision Log

| Decision | Choice | Rationale |
|---|---|---|
| Compute | Cloudflare Workers (`nodejs_compat`) | CPU limits enforced by CF plan, actions run directly on Workers |
| Database | D1 | Managed SQLite, built-in Time Travel |
| Real-time | Durable Objects + WebSocket | Hibernation API for efficient connections, bidirectional messaging |
| Storage | R2 | Native, zero egress, no plugin needed |
| Vector search | Vectorize (sidecar) | D1 doesn't support native vectors |
| Scheduler | SchedulerDO (Alarms + SQLite) for scheduled functions, CF Cron Triggers for recurring | No time limit, full job history, cancel support |
| Secrets | Worker Secrets | CF-managed encryption |
| Local dev | Miniflare (programmatic) | Full control, no wrangler dependency |
| Auth | better-auth v1.6 via custom document adapter + `defineAuth()` | Consistent document model for all tables, 1:1 better-auth config passthrough |
| Deploy | CF Workers API | No wrangler CLI dependency |
| Backups | D1 Time Travel | Built-in, 30-day retention |
| One Worker per env | Yes | True isolation, independent scaling |
| Dashboard | Local only (`npx baseflare dashboard`) | No hosted management infrastructure |
| License | MIT | Maximally permissive, encourages adoption |
| Data model | Document (JSON `_data` column) | Field changes are no-ops, same model as Convex-style document databases |
| Query API | `.filter()` only, no `.withIndex()` | SQLite query planner selects indexes automatically |
| Schema diffing | Tables + indexes only, orphaned tables kept read-only until deleted via dashboard | No data loss on deploy, fields naturally clean up on rewrite |
| ID format | Plain UUIDv7 strings | Time-sortable, universally parseable, no custom encoding, `get('table', id)` |
| File naming | kebab-case | Consistent, ecosystem standard |
| Core package | Single `baseflare` package with `./values`, `./server`, `./client`, and CLI subpaths | Eliminates version skew between core APIs while keeping imports tree-shakeable |
| Framework adapters | Separate packages such as `@baseflare/react` | Framework adapters carry mutually exclusive peer dependencies and should version independently |
| CLI command | One binary: `baseflare` | Simple install story through `npx baseflare` or package scripts; no short alias |
| Scaffolding | Future `baseflare new` command | Fresh projects get a new directory, existing projects use lazy prompts in `dev`/`deploy` |
