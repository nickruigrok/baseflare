# @baseflare/server

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
values throw. Non-paginated `.collect()` has no such restriction.
