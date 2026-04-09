# Contributing

This document currently describes the internal development workflow for Baseflare.

External pull requests are not being accepted yet. The project is still under active internal development, and contribution intake will open later once the package surfaces, runtime behavior, and repo workflow are stable enough to support outside contributors well.

## Workflow

- Create feature branches from `main`.
- Use clear branch names that describe the work, for example `feat/runtime-adapter` or `fix/query-builder-ordering`.
- Open pull requests back into `main`.
- Treat publishing as a separate manual release step. Normal pushes and merges do not publish packages.

## Local Verification

Use `pnpm` in this repository.

- `pnpm check` is the standard local verification flow and should pass before pushing.
- `pnpm build` should also be run when a change affects package exports, build output, packaging behavior, or release-facing docs.
- Individual commands:
  - `pnpm format`
  - `pnpm lint`
  - `pnpm typecheck`
  - `pnpm test`
  - `pnpm build`

## Linting Policy

Baseflare uses Ultracite as the baseline with a small set of library-oriented Biome overrides.

- Package root `src/index.ts` files are the public API boundary and may re-export intentionally.
- Internal subsystem folders should use direct module imports and exports instead of local barrel files.
- Low-level modules may use narrow file-scoped lint exceptions when they materially improve clarity.
- Top-level regex hoisting is not enforced; use the clearest local scope for each regex.

## Commits

Conventional Commits are enforced locally with `commitlint`.

Use standard commit types such as:

- `feat`
- `fix`
- `refactor`
- `docs`
- `test`
- `chore`
- `build`
- `ci`
- `perf`

Examples:

- `feat: add subscription runtime contract types`
- `fix: preserve createdAt when deserializing documents`
- `docs: tighten readme roadmap wording`

## Git Hooks

`lefthook` installs the local Git hooks through `pnpm prepare`.

- `pre-commit` runs Ultracite autofixes on staged files.
- `commit-msg` validates Conventional Commit messages.
- `pre-push` runs `pnpm typecheck` and `pnpm test`.

Hooks are a local safety net, not a substitute for running the full expected verification flow before pushing.

## Release Boundary

- Package publishing is currently manual.
- Merging to `main` does not publish anything automatically.
- Changesets remains the versioning source of truth when releases are prepared.
