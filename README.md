# konserve-s3

A backend for [konserve](https://github.com/replikativ/konserve) that supports Amazon [S3](https://aws.amazon.com/s3) and any S3-compatible storage API.


## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/org.replikativ/konserve-s3/latest-version.svg)](http://clojars.org/org.replikativ/konserve-s3)

### Configuration

``` clojure
(require '[konserve-s3.core]  ;; Registers the :s3 backend
         '[konserve.core :as k])

(def config
  {:backend :s3
   :region "us-west-1"
   :bucket "my-bucket"
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"
   ;; Optional:
   :access-key "your-access-key"
   :secret "your-secret"
   :endpoint-override {:protocol :https
                       :hostname "fly.storage.tigris.dev"}
   :x-ray? false})

(def store (k/create-store config {:sync? true}))
```

For API usage (assoc-in, get-in, delete-store, etc.), see the [konserve documentation](https://github.com/replikativ/konserve).

### Multiple Stores in Same Bucket

S3 supports multiple independent stores within the same bucket by using different `:id` values:

``` clojure
;; Store 1
(def store1-config
  {:backend :s3
   :region "us-west-1"
   :bucket "my-bucket"
   :id #uuid "11111111-1111-1111-1111-111111111111"})

;; Store 2 - same bucket, different ID
(def store2-config
  {:backend :s3
   :region "us-west-1"
   :bucket "my-bucket"
   :id #uuid "22222222-2222-2222-2222-222222222222"})

(def store1 (k/create-store store1-config {:sync? true}))
(def store2 (k/create-store store2-config {:sync? true}))

;; Each store maintains its own isolated namespace within the bucket
```

### Listing Stores in a Bucket

You can discover all konserve stores in a bucket using `list-stores`:

``` clojure
(require '[konserve-s3.core :as s3])

;; List all store IDs in a bucket
(def bucket-config
  {:region "us-west-1"
   :bucket "my-bucket"})

(s3/list-stores bucket-config :opts {:sync? true})
;; => #{#uuid "11111111-1111-1111-1111-111111111111"
;;      #uuid "22222222-2222-2222-2222-222222222222"}
```

### Conditional Writes (fencing) for Distributed Updates

konserve-s3 supports konserve's conditional writes, evaluated by S3 itself
(`If-Match` on the object's ETag), so a compare-and-set holds against **every**
writer anywhere — other machines, other processes, serverless invocations — with
no distributed lock. konserve calls this the `:global` conditional-write domain.

Fencing is something the **caller asks for**, per write, by handing back the
revision it read:

``` clojure
;; Read the value together with its revision token ...
(let [[value rev] (k/get store :counter nil {:sync? true :with-revision? true})]
  ;; ... and write only if the stored value is still that revision.
  (k/assoc store :counter (inc value) {:sync? true :expected-revision rev}))

;; Or read the revision on its own:
(k/revision store :counter {:sync? true})

;; A read-modify-write that must not lose updates: retry from a RE-READ revision
;; on conflict. Retrying against the same token would be rejected forever — the
;; point of the fence is that the value moved.
(loop []
  (let [rev (k/revision store :counter {:sync? true})]
    (when (= ::conflict
             (try (k/update-in store [:counter] (fnil inc 0)
                               {:sync? true :expected-revision rev})
                  (catch Exception e
                    (if (= :konserve/revision-mismatch (:type (ex-data e)))
                      ::conflict
                      (throw e)))))
      (recur))))

;; Create-if-absent: fence on the key NOT existing.
(k/assoc store :lease {:owner me} {:sync? true :expected-revision konserve.core/absent})
```

**How it works**

1. A fenced write reads the object once, taking both its metadata `:revision`
   and its S3 ETag from that single `GET`.
2. konserve compares the revision you passed against the one it read
   (`check-revision!`); a mismatch is rejected before anything is written.
3. The `PUT` carries `If-Match: <that ETag>` (or `If-None-Match: *` for
   create-if-absent), so S3 rejects it if the object changed between the read
   and the write — the half no client-side comparison can close.
4. A rejection surfaces as an `ex-info` with `:type :konserve/revision-mismatch`.
   It is **not retried by konserve**: the conflict belongs to you, since
   re-running your update function against a value you never saw is exactly
   the silent drift fencing exists to prevent. Re-read, and decide.

**What is guaranteed**

- An accepted `:expected-revision` write was applied to the object whose
  revision you passed — not to a replacement written in between, whatever else
  ran on the same store handle meanwhile (other reads, `keys` listings, other
  writes). The `If-Match` token is bound to the write's own read.
- Revision tokens are opaque and minted per write. Hold one, hand it back; do
  not compare or order them.

**What is not**

- `dissoc` cannot be fenced; konserve refuses `:expected-revision` on it.
- A key written by a konserve older than revisions carries none, and a fenced
  write to it is refused (`:konserve/revision-unavailable`). One unconditional
  write gives it a revision.
- `:in-place? false` is ignored on S3 (with a warning). S3 has no atomic
  rename — a "move" is `CopyObject` + `DeleteObject` — so rename mode costs two
  extra requests per write and would make every fenced write impossible. The
  store always runs in-place, where a `PUT` already replaces the object
  atomically.
- Unfenced writes are last-writer-wins, as on every konserve backend.

### Notes

Note that you do not need full S3 rights if you manage the bucket outside, i.e.
create it before and delete it after usage form a privileged account. Connection
will otherwise create a bucket and all files created by konserve (with suffix
".ksv", ".ksv.new", ".ksv.backup" or ".ksv.cas") will be deleted by
`delete-store`, but the
bucket needs to be separately deleted by `delete-bucket`. You can activate
[Amazon X-Ray](https://aws.amazon.com/xray/) by setting `:x-ray?` to `true` in
the S3 spec.

## ClojureScript (browser + Node)

A parallel **ClojureScript** backend ships in the same repo
(`konserve-s3.core`, `core.cljs`). It talks to the same S3-compatible APIs via
[`aws4fetch`](https://github.com/mhart/aws4fetch) + `fetch`, runs on Node ≥ 18
and in the browser, and is **async only** (`:sync? false`). It targets
**Amazon S3** and **Cloudflare R2** as first-class providers (and works with any
S3-compatible API — MinIO, Tigris, Backblaze B2, …).

### Install the npm peer dependency

The cljs backend requires the [`aws4fetch`](https://github.com/mhart/aws4fetch)
npm package at runtime. It is **not** pulled in transitively by the Clojars
artifact, so add it to your project's `package.json` yourself:

```bash
npm install aws4fetch
```

shadow-cljs resolves it from your `node_modules` at build time; without it the
build fails with `The required JS dependency "aws4fetch" is not available`.

```clojure
(require '[konserve-s3.core :as s3]
         '[konserve.core :as k]
         '[clojure.core.async :refer [go <!]])

(go
  (let [store (<! (s3/connect-s3-store
                   {:endpoint   "https://s3.us-west-1.amazonaws.com"
                    :bucket     "my-bucket"            ;; must already exist
                    :region     "us-west-1"
                    :access-key "…" :secret "…"
                    :id         (random-uuid)
                    ;; opt in to ETag CAS; without it update-in is last-write-wins
                    :config     {:optimistic-locking-retries 10}}
                   :opts {:sync? false}))]
    (<! (k/assoc-in store [:counter] 0 {:sync? false}))
    (<! (k/update-in store [:counter] inc {:sync? false})) ;; ETag CAS, safe
    (println "counter =" (<! (k/get-in store [:counter] nil {:sync? false})))))
```

`delete-s3-store` and `list-stores` mirror the API above, and the backend is
registered for `konserve.store`'s `:s3` dispatch. Optimistic locking works the
same way as on the JVM (`:config {:optimistic-locking-retries n}`) — it is what
makes `update-in` a safe cross-device CAS.

### Provider config (S3 vs. R2 vs. others)

The aws4fetch config is the same shape for every provider; only the endpoint and
region differ:

| Provider     | `:endpoint`                                          | `:region`     | `:path-style?` |
| ------------ | --------------------------------------------------- | ------------- | -------------- |
| Amazon S3    | `https://s3.<region>.amazonaws.com`                 | real region   | `false`        |
| Cloudflare R2| `https://<account-id>.r2.cloudflarestorage.com`     | `"auto"`      | `true`         |
| MinIO / B2 / Tigris | the provider's endpoint                      | their region  | `true`         |

The bucket must already exist; the cljs backend does not create buckets.

### CORS (browser only)

When connecting from a browser, the bucket needs a CORS policy. Node needs none.
The classic gotcha is **`ExposeHeaders: ETag`** — without it the browser can read
the response but `headers.get("etag")` returns `nil`, and optimistic locking
silently breaks.

```json
[
  {
    "AllowedOrigins": ["https://your-app.example"],
    "AllowedMethods": ["GET", "PUT", "DELETE", "HEAD"],
    "AllowedHeaders": ["authorization", "content-type",
                       "if-match", "if-none-match", "x-amz-*"],
    "ExposeHeaders": ["ETag"]
  }
]
```

### Browser credential caveat

aws4fetch signs requests with the access key/secret you pass it. **Do not embed
long-lived root credentials in client-side code.** Use short-lived scoped
credentials (e.g. STS session tokens via `:session-token`, or R2 scoped tokens)
minted by a backend you control.

### Building & testing the cljs backend

Network tests need a reachable S3-compatible bucket. Locally that's the
docker-compose MinIO (`docker compose up -d`, then create a `konserve-test`
bucket); the same suites run against MinIO in CI.

```bash
# Node: shared helpers + full async compliance + MinIO integration
# (store lifecycle, multi-store isolation, list-stores, optimistic locking)
npx shadow-cljs compile node-test && node target/node-tests.js

# Browser, network-free unit tests only (headless Chrome)
npx shadow-cljs release ci && CHROME_BIN=$(which chromium) \
  npx karma start --single-run

# Browser integration: compliance + cross-origin ETag optimistic locking
# against a live bucket (headless Chrome)
npx shadow-cljs release integration && CHROME_BIN=$(which chromium) \
  npx karma start karma.integration.conf.js --single-run
```

The Node tests read their endpoint from env vars (`S3_ENDPOINT`, `S3_BUCKET`,
`S3_ACCESS_KEY`, `S3_SECRET`, `S3_REGION`, `S3_PATH_STYLE`); the browser
integration test bakes the same config in at build time via `goog-define`
(override with `:closure-defines`). Both default to the docker-compose MinIO at
`localhost:9000`.

## Authentication

A [common approach](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)
to manage AWS credentials is to put them into the environment variables as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to avoid storing them in plain
text or code files. Alternatively you can provide the credentials in the
`s3-spec` as `:access-key` and `:secret`.

## License

Copyright © 2023-2026 Christian Weilbach

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
