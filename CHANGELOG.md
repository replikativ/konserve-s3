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
