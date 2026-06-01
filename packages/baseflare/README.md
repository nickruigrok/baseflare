# baseflare

Baseflare server APIs: schema, function wrappers, document database, permissions,
HTTP actions, and (later phases) the Cloudflare Worker runtime.

## Schema evolution & backfill

Baseflare uses a document model, so adding, removing, renaming, or retyping a
field needs **no migration** — only table and index changes touch D1.

- **Reads** return whatever is stored, including fields no longer in the schema.
- **`patch`** validates only the fields you change and strips fields that are no
  longer in the schema on rewrite. A document that predates a newly-required
  field can still be patched on unrelated fields — it will not fail validation.
- **`insert` / `replace`** validate the whole document against the current schema.

To backfill data (e.g. populate a new required field on old rows), write a
mutation/action that pages through the table and `patch`es each document. A
dedicated migrations primitive may come later; for now the patch-on-rewrite
pattern covers the common cases.

## Mutation semantics

Mutations are atomic from the application point of view. The runtime detects
write conflicts and retries mutation handlers when it can do so safely, so
mutation handlers must be deterministic and retry-safe.
Return value validation is part of that atomic contract: if a mutation returns
an invalid value, its pending writes are rolled back.

Keep external side effects in actions, not mutations. Actions are the right
place for network calls, payments, email, webhooks, and other work that should
not be retried automatically. Actions access the database by calling
`ctx.runQuery()` and `ctx.runMutation()`. Each `ctx.runMutation()` call is its
own mutation transaction, so put atomic multi-write workflows in one mutation.

Keep mutations focused on a bounded set of documents. For large datasets, use
selective filters and pagination; future bulk/import workflows will cover very
large write jobs explicitly.

## Query filter semantics

Object filters treat `null` as the JSON-safe nullish query value for document
fields. `{ field: null }` and `{ field: { eq: null } }` match explicit JSON
`null` and absent optional fields. `{ field: { neq: null } }` matches only
present non-null values, while `{ field: { in: [null, ...] } }` includes
explicit `null` and absent fields. Do not use `undefined` in filters; it is not
JSON/RPC-safe.

## Query ordering semantics

`.order(field, direction)` sorts by `json_extract(_data, '$.field')` with `_id`
as a stable tiebreak. Ordering follows SQLite / `json_extract` sort semantics:
`NULL` (missing or explicitly null fields) sorts before all other values in
ascending order, and remaining values follow SQLite's storage-class ordering.
Mixed-type or sparse fields therefore sort predictably but may surprise callers
expecting a single JS type order. `.order("_createdAt", …)` is equivalent to the
default `_id` ordering (UUIDv7 encodes creation time).

For paginated, field-ordered queries the ordered value must be a scalar
(`string`, `number`, `boolean`, or `null`/missing); array, object, or bytes
values throw. Mixed scalar values follow SQLite storage-class ordering, but
homogeneous ordered fields are recommended for predictable application behavior.
Non-paginated `.collect()` has no such restriction.

Pagination cursors are opaque strings that represent a position in a query
result. They are not signed or encrypted, so callers must not parse them or rely
on them for access control. Enforce authorization through permissions and query
filters; treat cursors only as pagination state.
