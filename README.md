# Baseflare

Baseflare is a Cloudflare-native backend framework with a Convex-style developer experience. It is designed around a typed document model, server function primitives, and direct use of Cloudflare building blocks instead of a hosted control plane.

The long-term goal is straightforward: define your schema, write typed queries and mutations, deploy directly to Cloudflare, and consume the result through generated client and framework bindings with real-time behavior built in.

> [!WARNING]
> Baseflare is early-stage alpha software. Core schema, validation, query, permission, configuration, and routing primitives are implemented today. The Cloudflare runtime layer, CLI workflows, auth, frontend SDKs, and dashboard are still in progress.

## What Baseflare Is

Baseflare is intended to provide:

- Typed schema definitions built from validators
- Typed query, mutation, action, and HTTP action primitives
- A document-oriented model on top of Cloudflare infrastructure
- Direct deployment to Cloudflare without a hosted Baseflare control plane
- A future full-stack workflow with generated clients, framework bindings, real-time subscriptions, and local management tooling

The developer experience is inspired by Convex, but the architecture is Cloudflare-native from the start.

## What Exists Today

The currently implemented core lives primarily in `@baseflare/values` and `@baseflare/server`.

### Shared typed core

- Typed validation and shared value contracts
- Document IDs with time ordering and created-at derivation
- Shared error, pagination, and transport primitives

### Server-side core

- Schema definition and schema diffing
- Query and write-path core primitives
- Document serialization and validation model
- Permission rules with deny-by-default behavior
- HTTP routing primitives
- Core server-side configuration and interfaces

## What Is Planned Next

Baseflare is intended to grow into the full Cloudflare-native application platform described in [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md).

Current roadmap areas:

- Cloudflare Worker runtime and adapters for D1, R2, Durable Objects, and Vectorize
- Real-time subscriptions and scheduler runtime
- Local-first CLI workflows for init, dev, deploy, codegen, and environment management
- Auth
- Generated client SDK and React integration
- Local dashboard for inspecting and managing environments

The full technical roadmap, architectural decisions, and package-level specifications live in [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md).

## How Baseflare Works

The intended architecture looks like this:

1. You define schema and server functions in application code.
2. A Cloudflare runtime layer executes those functions against D1, R2, Durable Objects, and other platform services.
3. Generated clients and framework bindings consume the typed server surface.
4. The CLI and local dashboard manage environments directly through Cloudflare APIs.

Today, the core schema, validation, query, permission, and routing pieces exist. The runtime, generated client surface, framework integrations, and management tooling are still being built.

## Packages

| Package | Purpose | Current status |
| --- | --- | --- |
| `@baseflare/values` | Shared typed core: validation, value contracts, IDs, and transport types | Core implemented |
| `@baseflare/server` | Server-side core: schema, queries, validation, permissions, routing, and config | Core implemented |
| `@baseflare/client` | Typed client SDK for browser and Node environments | Planned |
| `@baseflare/react` | React bindings on top of the client SDK | Planned |
| `@baseflare/cli` | CLI for bootstrap, local development, deploy, codegen, and environment workflows | Scaffolded, workflow planned |
| `@baseflare/dashboard` | Local dashboard for environment management and data inspection | Private package, planned |

## Getting Started

The honest way to work with Baseflare today is as a repo and library project under active development.

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

### Planned App Bootstrap UX

The intended end-user entrypoint is:

```bash
npx @baseflare/cli init my-app
```

That CLI experience is part of the planned workflow, but it should not be treated as production-ready yet.

## Development

This repository is a monorepo containing:

- `packages/values` → `@baseflare/values`
- `packages/server` → `@baseflare/server`
- `packages/client` → `@baseflare/client`
- `packages/react` → `@baseflare/react`
- `packages/dashboard` → `@baseflare/dashboard`
- `packages/cli` → `@baseflare/cli`

For implementation details, package boundaries, and roadmap decisions, use [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) as the technical source of truth.

Internal development workflow, linting rules, and commit conventions are documented in [CONTRIBUTING.md](./CONTRIBUTING.md).

## Project Status

Baseflare is not production-ready yet.

What exists today is the core library layer: validation, schema definition, query and mutation primitives, permissions, document serialization, write validation, and HTTP routing. The platform/runtime layer and developer-product layer are still under active development.

## Contributing

External contributions are not being accepted at this time.

The project is still moving quickly, and implementation is currently being driven internally. Public contribution intake will open later once the package surfaces, runtime behavior, and workflow are stable enough to support outside contributions well.

`CONTRIBUTING.md` exists for the current internal workflow and will become relevant to outside contributors once contribution intake opens.
