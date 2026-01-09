# konserve-s3

A backend for [konserve](https://github.com/replikativ/konserve) that supports Amazon [S3](https://aws.amazon.com/s3) and any S3-compatible storage API.


## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/konserve-s3/latest-version.svg)](http://clojars.org/io.replikativ/konserve-s3)

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

## Authentication

A [common
approach](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)
to manage AWS credentials is to put them into the environment variables as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to avoid storing them in plain
text or code files. Alternatively you can provide the credentials in the
`s3-spec` as `:access-key` and `:secret`.

## Commercial support

We are happy to provide commercial support with
[lambdaforge](https://lambdaforge.io). If you are interested in a particular
feature, please let us know.

## License

Copyright © 2023 Christian Weilbach

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
