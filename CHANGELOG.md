# Changelog

All notable, user-visible changes to konserve-s3 are documented here.

## Unreleased

### Added
- **Read-miss-safe reads (single `GET`, no `HEAD`).** The S3 backing implements
  konserve's `PReadMissSafe` and its `-read-header` throws
  `store-key-not-found-ex` on an absent object. On a konserve that supports the
  marker, a read is a single `GET` (the redundant `HEAD` existence probe is
  dropped), and read-modify-write ops (`update-in` / `assoc-in` / `bassoc`) skip
  the `HEAD` too — a hit goes from `HEAD` + `GET` + `PUT` to `GET` + `PUT`.
  Requires konserve with `PReadMissSafe` (older konserve simply keeps the probe).
- **`dissoc` with `:ignore-existence? true` skips the `HEAD`.** `DeleteObject` is
  idempotent, so a caller that does not need the existed?/false-for-missing return
  can delete in a single request. konserve-s3 is single-key, so konserve's GC
  sweep takes this path — each dead-object delete is one `DELETE` instead of
  `HEAD` + `DELETE`. The default `dissoc` still probes to honour the contract.
- **`with-io-stats` now covers `LIST`.** `ListObjectsV2` was the one S3 op that
  went unmeasured, so `:list` is a new entry in the summary — `:n`, `:total-ms`,
  `:p50-ms`, `:p99-ms` — and it additionally records `:items`, the objects the
  responses carried. A listing scoped to one store and a whole-bucket one can both
  be a single request while differing by orders of magnitude in what they
  transfer, so the request count alone cannot show a listing that is not scoped to
  its store.

### Fixed
- **`delete-store` left konserve's fenced-write lock sidecar behind.** konserve's
  `.cas` sidecar (`konserve.impl.defaults/cas-lock-suffix`, konserve 0.9.376+) is
  permanent, and its `internal-artifact?` requires a backend that filters
  enumeration itself — this one does — to recognise the suffix. `store-file?` did
  not, so such an object would survive `-delete-store`: a store reporting itself
  deleted with one object per fenced key still in the bucket. Not reachable in any
  released version (konserve only takes the sidecar when the backing does not
  declare `PSelfConditionalWrite`, and both backends have declared it since the
  same release that first pinned a konserve with `.cas`), so this closes it before
  it can be opened — by dropping that declaration, or by wrapping the backing in
  one that does not re-declare it. `data-key?` still excludes the sidecar: it is
  konserve's bookkeeping, not a key.

- **`keys` and `delete-store` listed the whole bucket (JVM) / the bare store-id
  prefix (cljs) instead of the store's own objects.** On the JVM, `-keys` and
  `-delete-store` called `ListObjectsV2` with **no prefix** and filtered by
  store-id client-side, so enumerating one store paged every object of every
  *other* store in the bucket into the client — one request per 1000 bucket
  objects, on every call, growing with each unrelated store added. Measured on a
  bucket of 3.8M objects holding ~1000 stores: a `keys` call on a 3465-object
  store issued ~3833 `ListObjectsV2` requests and pulled ~1 GB to keep 0.09% of
  it, taking ~150s — so `keys` timed out before reading a single value. Every
  object of a store already shares the prefix `<store-id>_` (`storage/->key`,
  and `storage/marker-key`, whose suffix starts with `_`), so S3 can do the
  filtering: `list-objects` takes an optional `prefix`, and both backends pass
  the new `storage/store-prefix`. Cost is now proportional to the store (~4
  requests for that store instead of ~3833). `list-stores` still scans the bucket
  unprefixed — finding every store's marker is genuinely a bucket-wide question.
  No key-layout change, no migration.
- **A store-id that was a prefix of another's saw, read and deleted its data.**
  `data-key?` / `store-file?` matched `(str/starts-with? key store-id)` with no
  separator, so with stores `test` and `test2` in one bucket, `test`'s `keys`
  returned `test2`'s blobs and `delete-store` on `test` **deleted** them. Scoping
  the listing to `<store-id>_` narrows that but does not close it: `test_2`'s
  objects genuinely sit under `test_`. And the leaked store-key is not inert —
  `->key` maps it straight back onto the neighbour's real object, so the outer
  store could **read** its neighbour's values. Both predicates now go through
  `storage/store-key`, which requires the prefix and rejects a remainder
  containing `_` (konserve store-keys are a UUID plus suffix, so they never
  contain one). Reachable only through the backends' own `connect-store` /
  `connect-s3-store`, which take `(str (:id s3-spec))` — `konserve.store`
  enforces a UUID `:id`, and UUID store-ids are structurally immune.

- **`delete-store` deleted nothing on the async path.** `-delete-store :s3` returned
  its inner `delete-store` call *without awaiting it*, so under `{:sync? false}` —
  which is `konserve.store/delete-store`'s **default**, and what Datahike's
  `d/delete-database` uses — the caller got back an un-awaited channel, the objects
  were never removed, `store-exists?` kept returning `true`, and any error was
  swallowed into a channel nobody read. The three sibling methods (`-connect-store`,
  `-create-store`, `-store-exists?`) all await their inner call; this one did not.
  Deleting a store (offboarding a tenant, GDPR erasure) was silently a no-op on S3.
  Every existing `delete-store` test passed `{:sync? true}`, which is why it went
  unnoticed — the regression test added here is deliberately async.
