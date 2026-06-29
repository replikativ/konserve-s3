# konserve-s3

A backend for [konserve](https://github.com/replikativ/konserve) that supports Amazon [S3](https://aws.amazon.com/s3) and any S3-compatible storage API.


## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/org.replikativ/konserve-s3/latest-version.svg)](http://clojars.org/org.replikativ/konserve-s3)

### Configuration

``` clojure
(require '[org.replikativ.konserve-s3.core]  ;; Registers the :s3 backend
         '[org.replikativ.konserve.core :as k])

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
(require '[org.replikativ.konserve-s3.core :as s3])

;; List all store IDs in a bucket
(def bucket-config
  {:region "us-west-1"
   :bucket "my-bucket"})

(s3/list-stores bucket-config :opts {:sync? true})
;; => #{#uuid "11111111-1111-1111-1111-111111111111"
;;      #uuid "22222222-2222-2222-2222-222222222222"}
```

### Optimistic Locking for Distributed Updates

konserve-s3 supports optimistic concurrency control using S3's ETag-based conditional writes. This enables safe concurrent updates from multiple machines without distributed locks.

``` clojure
;; Enable optimistic locking with up to 10 retries on conflict
(def config
  {:backend :s3
   :region "us-west-1"
   :bucket "my-bucket"
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"
   :config {:optimistic-locking-retries 10}})

(def store (k/create-store config {:sync? true}))

;; Now update-in is safe across multiple machines!
;; Each machine can run this concurrently:
(k/update-in store [:counter] (fnil inc 0) {:sync? true})
```

**How it works:**
1. When reading a key, konserve-s3 captures the object's ETag (a hash of the content)
2. When writing, it uses S3's `If-Match` header with the captured ETag
3. If another process modified the object, S3 returns HTTP 412 (Precondition Failed)
4. konserve automatically retries: re-reads the new value, re-applies your update function, and writes again
5. This continues until the write succeeds or max retries is exceeded

This is particularly useful for:
- Counters and metrics aggregation across distributed workers
- Shared configuration that multiple services update
- Any read-modify-write pattern in distributed systems

**Note:** Without optimistic locking enabled, concurrent `update-in` calls from different machines may lose updates (last-write-wins). With optimistic locking, all updates are preserved through automatic retry.

### Notes

Note that you do not need full S3 rights if you manage the bucket outside, i.e.
create it before and delete it after usage form a privileged account. Connection
will otherwise create a bucket and all files created by konserve (with suffix
".ksv", ".ksv.new" or ".ksv.backup") will be deleted by `delete-store`, but the
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

```clojure
;; deps.cljs already declares the aws4fetch npm dep; no manual install needed.
(require '[konserve-s3.core :as s3]
         '[konserve.core :as k]
         '[clojure.core.async :refer [go <!]])

(go
  (let [store (<! (s3/connect-s3-store
                   {:endpoint   "https://s3.us-west-1.amazonaws.com"
                    :bucket     "my-bucket"            ;; must already exist
                    :region     "us-west-1"
                    :access-key "…" :secret "…"
                    :id         (random-uuid)}
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

```bash
npx shadow-cljs compile node-test && node target/node-tests.js   # Node + compliance
npx shadow-cljs release ci && CHROME_BIN=$(which chromium) \
  npx karma start --single-run                                   # browser (headless)
```

The compliance suite runs against an env-configured endpoint (`S3_ENDPOINT`,
`S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET`, `S3_REGION`, `S3_PATH_STYLE`),
defaulting to the docker-compose MinIO at `localhost:9000`.

## Authentication

A [common approach](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)
to manage AWS credentials is to put them into the environment variables as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to avoid storing them in plain
text or code files. Alternatively you can provide the credentials in the
`s3-spec` as `:access-key` and `:secret`.

## License

Copyright © 2023-2026 Christian Weilbach

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
