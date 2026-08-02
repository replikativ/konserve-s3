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

### Fixed
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
