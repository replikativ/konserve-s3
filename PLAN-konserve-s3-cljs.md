# konserve-s3-cljs — Implementation Plan

A ClojureScript (browser + Node) konserve backend for S3-compatible object
storage, developed against Cloudflare R2. Request signing via **aws4fetch**
(decided — no hand-rolled SigV4). Standalone library, developed in parallel
to ivylee; ivylee adopts it in its milestone 5.

Move this file into the library's own repo once created (suggested: sibling
dir `~/projects/konserve-s3-cljs`).

## Orientation: how konserve backends work

The high-level API (`get-in`, `assoc-in`, `update-in`, …) is implemented once
in `konserve.impl.defaults`; a backend only supplies the protocols from
`konserve.impl.storage-layout`:

- **`PBackingStore`** — store/blob lifecycle: `-create-blob`, `-delete-blob`,
  `-blob-exists?`, `-copy`, `-atomic-move`, `-create-store`, `-delete-store`,
  `-store-exists?`, `-sync-store`, `-keys`, plus migration hooks
  (`-migratable`, `-migrate`, `-handle-foreign-key`).
- **`PBackingBlob`** — one open blob: `-read-header` / `-read-meta` /
  `-read-value` / `-read-binary` and the corresponding `-write-*`, plus
  `-sync`, `-close`, `-get-lock`.
- **`PBackingLock`** — `-release`.
- Optional: `PMultiWriteBackingStore` / `PMultiReadBackingStore` — skip
  initially, add later if useful.

A stored value is `header (20 bytes) ‖ meta ‖ value`. The defaults layer
drives the read/write call sequence; the backend just moves bytes.

**Reference implementations** (read before coding):
- `konserve/src/konserve/indexeddb.cljs` — the cljs async backend closest in
  shape (promise→core.async plumbing, ArrayBuffer handling, `connect-store`).
- `konserve-s3/src/konserve_s3/core.clj` — the S3 mapping to mirror (JVM).

## Design decisions (mirror the JVM backend unless noted)

1. **One S3 object per konserve key**: `{store-id}_{key}.ksv`, body is the
   concatenated header‖meta‖value. The blob record buffers the three parts in
   an atom during `-write-*` calls; `-sync` concatenates into one Uint8Array
   and PUTs. (`.ksv.new` / `.ksv.backup` suffixes appear during
   copy/atomic-move, as in the JVM backend.)
2. **Optimistic locking via ETags**: `-read-header` GETs the object and
   captures the ETag; `-sync` PUTs with `If-Match: etag` (or
   `If-None-Match: *` for fresh creates). On 412, the defaults/locking layer
   retries — expose `:opts {:optimistic-locking-retries n}` like the JVM
   backend. This is what makes `update-in` a safe CAS and the ivylee sync
   loop a one-liner.
3. **`-atomic-move`** = S3 CopyObject (`PUT` with `x-amz-copy-source`) +
   DeleteObject. S3 has no rename; copy+delete is what the JVM backend does.
4. **`-keys`** = ListObjectsV2 with the store-id prefix, filter `.ksv*`
   suffixes, strip prefix. Response is XML: parse with `DOMParser` in the
   browser; in Node (no DOMParser) extract `<Key>` elements with a small
   hand parser — only `Key`, `ETag`, `IsTruncated`/`NextContinuationToken`
   are needed. No XML library dependency.
5. **Store registry**: mirror the JVM backend's `_konserve-stores-registry`
   object and `{store-id}_.konserve-metadata` marker (ETag-CAS'd) so stores
   are discoverable and `-store-exists?` works.
6. **Binary values**: Uint8Array end-to-end; `-read-binary` hands the caller
   a `js/Blob`/ArrayBuffer via `locked-cb`.
7. **Serialization**: whatever `konserve.indexeddb` defaults to in cljs —
   do not invent anything; the compliance suite will catch mismatches.
8. **Async only**: like the IndexedDB backend, support `:opts {:sync? false}`
   only. Document it.
9. **Header constant**: reuse `konserve.impl.storage-layout/header-size` (20)
   and the defaults' header encoding — never re-implement.

## R2 specifics

- Endpoint `https://<account-id>.r2.cloudflarestorage.com`, region `"auto"`,
  service `"s3"` — aws4fetch config:
  `(AwsClient. #js {:accessKeyId … :secretAccessKey … :service "s3" :region "auto"})`.
- **Verify conditional writes in the spike** (load-bearing): `If-Match` and
  `If-None-Match: *` on PUT must return 412 on mismatch through R2's S3 API.
- **Bucket CORS** (browser only; Node needs none):
  - `AllowedMethods`: GET, PUT, DELETE, HEAD
  - `AllowedHeaders`: `authorization`, `content-type`, `if-match`,
    `if-none-match`, `x-amz-*`
  - `ExposeHeaders`: **`ETag`** — the classic gotcha: without it the browser
    can read the response but `headers.get("etag")` returns nil and locking
    silently breaks.
- ListObjectsV2 from the browser = GET on the bucket root; covered by the
  CORS rule above.

## Phases

**0. Spike (~half a day, throwaway code).** From a Node REPL with aws4fetch
against a scratch R2 bucket: PUT/GET round-trip, ETag capture, `If-Match`
happy/412 paths, `If-None-Match: *`, CopyObject, DeleteObject,
ListObjectsV2 + pagination. Write the findings into this file. Everything
later builds on these behaviors being confirmed.

**1. Scaffold.** New repo: `deps.edn` (dep: `org.replikativ/konserve`),
`shadow-cljs.edn` with a `:node-test` build, `package.json` with `aws4fetch`,
and a `deps.cljs` with `:npm-deps {"aws4fetch" "…"}` so consumers get the npm
dep resolved automatically. Node ≥ 18 (global `fetch` + WebCrypto, which
aws4fetch needs).

**2. Low-level S3 namespace** (`konserve-s3.s3` or similar): thin
promise→core.async wrappers over `client.fetch`: `get-object` (returns
`[bytes etag]`), `put-object` (optional `:if-match`/`:if-none-match`,
returns etag or `::conflict`), `delete-object`, `copy-object`,
`head-object`, `list-objects` (handles continuation tokens). This namespace
is independently testable against R2 before any konserve wiring exists —
build and REPL-test it first.

**3. Backend namespace**: `S3BackingStore` + `S3Blob` records implementing
the protocols per the design above, plus `connect-s3-store` /
`delete-s3-store` mirroring the IndexedDB backend's public API shape.

**4. Compliance.** Run `konserve.compliance-test` (it ships in konserve's
`src/`, designed for exactly this) under the `:node-test` build against R2.
Iterate until green — this is the milestone that makes it a real konserve
backend. Also run konserve's shared `tests/*.cljc` suites where applicable
(serializers, encryptor, gc).

**5. Browser.** Karma browser-test build (copy konserve's own
`karma.conf.js` setup), document the CORS config, then a smoke test from
ivylee: `connect-s3-store` + `update-in` with `merge-docs` from two
simulated devices.

**6. Publish.** README (R2 quickstart, CORS block to paste, credential
caveats for browser apps), CI with a scratch bucket (creds via secrets),
Clojars release. Open the upstream issue at replikativ: cljs support inside
`konserve-s3` vs. sibling library — offer to donate it either way.

## Testing policy

- **Test against real R2, not an emulator.** MinIO/LocalStack conditional-
  write semantics differ from R2, and R2's behavior is precisely what's
  being validated. A scratch bucket on the free tier costs nothing.
- Isolate runs: random store-id (UUID) per test run, `delete-store` in
  teardown, so parallel/aborted runs can't collide.
- Keep one tagged "slow" suite hitting the network; everything pure (byte
  layout, XML parsing, header encoding) gets fast local tests.

## Open questions (resolve during spike/compliance)

- Does the defaults layer require `-get-lock` to return a real lock for
  async stores, or is a no-op lock acceptable (check what IndexedDB backend
  returns)?
- Exact `.ksv.new`/`.ksv.backup` dance during `-atomic-move` — confirm
  against konserve-s3 JVM source rather than assuming.
- ListObjectsV2 pagination edge: >1000 keys per store (unlikely for ivylee,
  must still be correct for a published library).
