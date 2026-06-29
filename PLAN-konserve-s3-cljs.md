# konserve-s3-cljs — Implementation Plan

A ClojureScript (browser + Node) konserve backend for **S3-compatible object
storage**. It targets **Amazon S3** and **Cloudflare R2** as first-class
providers (and works with any S3-compatible API — MinIO, Tigris, Backblaze B2,
etc.). Request signing via **aws4fetch** (decided — no hand-rolled SigV4).

aws4fetch is a generic SigV4 signer, so the *same code* talks to Amazon S3 and
R2; only endpoint + region configuration differs (see "Provider specifics"
below). The plan is written provider-neutral; where R2 once appeared as the
sole target, both providers are now called out.

This backend is the cljs sibling of the existing JVM backend in
`src/konserve_s3/core.clj`. The two are intended to live **side by side** in
this repo with shared konserve-mapping logic where the platforms allow it — see
"Relationship to the JVM backend & code reuse".

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
   backend. This is what makes `update-in` a safe cross-device CAS and keeps a
   read-modify-write sync loop a one-liner.
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

## Relationship to the JVM backend & code reuse

The repo already ships a working JVM backend (`src/konserve_s3/core.clj`,
AWS Java SDK v2). The cljs backend is a **parallel implementation**, not a
replacement. They split into a layer that cannot be shared and a layer that
can:

**Platform-specific (cannot be shared):**
- The S3 I/O itself: AWS Java SDK (`S3Client`, `PutObjectRequest`,
  `ByteArrayOutputStream`, `byte[]`) on the JVM vs. aws4fetch + `fetch` with
  `Uint8Array` on cljs.
- JVM supports sync **and** async (`async+sync`); cljs is async-only.

**Shareable (the konserve-mapping logic — identical in shape on both):**
- Key naming: `->key`, `marker-key`, `registry-key` (pure string ops).
- The `.ksv` / `.ksv.new` / `.ksv.backup` suffix predicates used by `-keys`
  and `-delete-store`.
- The registry CAS algorithm (`update-registry`: read-with-etag → modify →
  conditional PUT → retry on conflict).
- The blob buffering contract (`-write-*` stash header/meta/value into an
  atom; `-sync` concatenates and conditionally PUTs).
- The record/protocol skeleton.

**Reuse strategy — a thin platform S3-ops abstraction.** Factor the S3 calls
behind a small set of functions (a protocol or a map of fns):
`get-object-with-etag` (→ `[bytes etag]`), `put-object`,
`put-object-conditional` (`:if-match` / `:if-none-match`, → ok | `::conflict`),
`delete`, `delete-keys`, `copy`, `exists?`/`head`, `list-objects` (paginated).
The konserve records, registry logic, and key/suffix helpers then live in a
`.cljc` that depends only on this abstraction; each platform supplies just the
~8 I/O functions (JVM via the Java SDK, cljs via aws4fetch).

**Phasing (to avoid destabilizing the shipped JVM backend):**
1. **[DONE]** Extract the genuinely pure helpers into a `.cljc` and have the
   JVM backend consume them, so there's one source of truth before any cljs
   code exists. See "Progress" below.
2. Build the cljs backend (`core.cljs`) against the shared helpers, supplying
   the aws4fetch S3-ops implementation.
3. Once the cljs backend passes compliance, *optionally* lift more into the
   shared `.cljc` — the registry CAS loop and the blob/record skeleton —
   behind the S3-ops abstraction, converging the two backends. Re-run the JVM
   compliance suite to prove no regression before committing to this.

Layout (✔ = exists):
```
src/konserve_s3/
  storage.cljc        ✔ shared pure helpers: key naming, suffix preds, registry ser/de
  core.clj            ✔ JVM backend (AWS Java SDK S3-ops; now consumes storage.cljc)
  core.cljs             cljs backend (aws4fetch S3-ops impl) + connect-store — TODO
```
Keep the public API shape aligned across both (`connect-store`,
`delete-store`, `list-stores`, and the `konserve.store` `:s3` multimethods),
differing only in the spec keys each platform needs.

### Progress

**Phase 1 — shared pure helpers (done).** `src/konserve_s3/storage.cljc`
created and `core.clj` refactored to consume it (no duplication left):
- Key naming: `->key`, `marker-key`, `registry-key`.
- Suffix predicates: `data-key?` (.ksv/.ksv.new/.ksv.backup, used by `-keys`)
  and `store-file?` (blobs + metadata marker, used by `-delete-store`),
  replacing the inlined `.startsWith`/`.endsWith` filters. Verified
  `store-file?` excludes the shared `registry-key` so `delete-store` can't
  wipe the registry.
- Registry ser/de: `serialize-registry` / `deserialize-registry`.
- Portability: byte↔string via reader conditionals (`.getBytes`/`String.` on
  JVM, `js/TextEncoder`/`js/TextDecoder` on cljs); registry parsing switched
  from `clojure.core/read-string` to an EDN reader (`clojure.edn` on clj,
  `cljs.reader` on cljs) — portable, safer, parses the `[#uuid …]` vector
  identically so JVM behavior is unchanged; suffix preds use
  `clojure.string/starts-with?`/`ends-with?` instead of Java `String` methods.
- Verified: both namespaces compile on the JVM and the helpers round-trip
  (UUID registry, key naming, predicates). **Not yet run:** the MinIO/network
  integration suite (`bin/test-minio.sh`) — changes are behavior-preserving by
  construction, but an end-to-end run is still pending.

The registry CAS loop (`update-registry`) and the `S3Blob`/`S3Bucket` records
stay in `core.clj` for now; they're candidates for phase 3 once cljs exists.

**Phases 2–4 — cljs backend + compliance against MinIO (done).**
- `core.cljs` exists with both layers: the aws4fetch S3-ops
  (`get-object`/`put-object`/`delete-object`/`copy-object`/`head-object`/
  `list-objects` with continuation-token paging and a no-XML-lib
  `parse-list-xml`) and the konserve backend (`S3Blob`/`S3BackingStore`,
  `connect-s3-store`/`delete-s3-store`/`list-stores`, `konserve.store` `:s3`
  multimethods). Compiles clean under shadow's `:node-test` (0 warnings).
- `src/deps.cljs` added so downstream consumers resolve the `aws4fetch` npm dep
  automatically (Phase 1 leftover).
- Tests (`test/konserve_s3/`): `storage_test.cljc` (pure helpers, runs on JVM +
  Node), `parser_test.cljs` (ListObjectsV2 XML parser), and
  `compliance_test.cljs` running konserve's `async-compliance-test` against an
  env-configured endpoint (defaults to the docker-compose MinIO; fresh
  `random-uuid` store-id per run, `delete-store` teardown in a `finally`).
- **Green: `node target/node-tests.js` → 35 assertions, 0 failures** (incl. the
  full async compliance suite) against local MinIO, and the shared `.cljc`
  helper tests pass on the JVM too (14 assertions).
- Note: this validates against MinIO. Per the testing policy, conditional-write
  (ETag CAS) semantics still need verification against **real Amazon S3 and R2**
  before publish — that's the remaining Phase 4 work (CI with scratch buckets +
  secrets). MinIO required pre-creating the `konserve-test` bucket; the backend
  assumes the bucket already exists.

**Phase 5 — browser build (build + headless run done; CORS documented).**
- shadow-cljs.edn gained `:browser-tests` (`:browser-test`, dev-http on 8021)
  and `:ci` (`:karma`, `:advanced`) builds; both exclude the node-only
  `konserve-s3.compliance-test` via negative-lookahead `:ns-regexp`. `karma.conf.js`
  mirrors konserve's; karma devDeps added to package.json.
- Verified: `:browser-tests` and the `:advanced` `:ci` build compile with 0
  warnings, and `karma start --single-run` runs the network-free suite (parser +
  shared storage helpers) in headless Chromium — **6 tests SUCCESS**.
- README now documents both S3 and R2 quickstarts, the provider config table,
  the **CORS block** (incl. the `ExposeHeaders: ETag` gotcha) and browser
  credential caveats.
- Still open for Phase 5: the live browser smoke test against a real
  CORS-configured bucket (a concurrent read-modify-write from two simulated
  devices) — needs real creds + CORS, same gate as the real-S3/R2 part of
  Phase 4.

## Provider specifics (Amazon S3, R2, and others)

aws4fetch config is the same shape for every provider; the differences are
endpoint and region:

- **Amazon S3**: default endpoint (no `endpoint` override needed), real region
  (e.g. `"us-west-1"`), service `"s3"`:
  `(AwsClient. #js {:accessKeyId … :secretAccessKey … :service "s3" :region "us-west-1"})`.
- **Cloudflare R2**: endpoint `https://<account-id>.r2.cloudflarestorage.com`,
  region `"auto"`, service `"s3"`:
  `(AwsClient. #js {:accessKeyId … :secretAccessKey … :service "s3" :region "auto"})`,
  passing the account endpoint as the request URL base.
- **Others** (MinIO, Tigris, B2): explicit endpoint + their region; path-style
  addressing may be required (mirror the JVM backend's `:path-style-access?`).

- **Verify conditional writes in the spike** (load-bearing): `If-Match` and
  `If-None-Match: *` on PUT must return 412 on mismatch — confirm on **both**
  Amazon S3 and R2 (Amazon S3 added `If-Match`/`If-None-Match` on PUT in
  2024; the JVM backend's `deps.edn` notes the SDK bump for exactly this).
- **Bucket CORS** (browser only; Node needs none) — applies to whichever
  provider serves the browser:
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
against a scratch bucket: PUT/GET round-trip, ETag capture, `If-Match`
happy/412 paths, `If-None-Match: *`, CopyObject, DeleteObject,
ListObjectsV2 + pagination. Run it against **both Amazon S3 and R2** (same
code, two configs) and note any behavioral differences. Write the findings
into this file. Everything later builds on these behaviors being confirmed.

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
is independently testable against Amazon S3 or R2 before any konserve wiring
exists — build and REPL-test it first. It is the cljs half of the platform
S3-ops abstraction described under "Relationship to the JVM backend".

**3. Backend namespace**: `S3BackingStore` + `S3Blob` records implementing
the protocols per the design above, plus `connect-s3-store` /
`delete-s3-store` mirroring the IndexedDB backend's public API shape.

**4. Compliance.** Run `konserve.compliance-test` (it ships in konserve's
`src/`, designed for exactly this) under the `:node-test` build against both
**Amazon S3 and R2** (parameterize the test config over providers). Iterate
until green — this is the milestone that makes it a real konserve backend.
Also run konserve's shared `tests/*.cljc` suites where applicable
(serializers, encryptor, gc).

**5. Browser.** Karma browser-test build (copy konserve's own
`karma.conf.js` setup), document the CORS config, then a smoke test:
`connect-s3-store` + a concurrent `update-in` from two simulated devices
(exercising the ETag CAS retry path end-to-end in a browser).

**6. Publish.** README with **both Amazon S3 and R2 quickstarts** (config
table showing the endpoint/region difference), CORS block to paste, credential
caveats for browser apps; CI with scratch buckets on both providers (creds via
secrets), Clojars release. Open the upstream issue at replikativ: cljs support
inside `konserve-s3` vs. sibling library — offer to donate it either way.

## Testing policy

- **Test against real Amazon S3 and R2, not an emulator.** MinIO/LocalStack
  conditional-write semantics differ from both, and conditional-write behavior
  is precisely what's being validated. Run the network suite against both
  providers so a provider-specific regression can't hide. A scratch bucket on
  the free tier costs nothing.
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
- ListObjectsV2 pagination edge: >1000 keys per store (uncommon, but must
  still be correct for a published library).
