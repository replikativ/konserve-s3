# konserve-s3

A backend for [konserve](https://github.com/replikativ/konserve) that supports Amazon [S3](https://aws.amazon.com/s3) and any S3-compatible storage API.


## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/konserve-s3/latest-version.svg)](http://clojars.org/io.replikativ/konserve-s3)

### Example

For asynchronous execution take a look at the [konserve example](https://github.com/replikativ/konserve#asynchronous-execution).


``` clojure
(require '[konserve-s3.core :refer [connect-s3-store]]
         '[konserve.core :as k])

(def s3-spec
  {:region "us-west-1"
   :bucket "konserve-demo"
   :store-id "test-store" ;; allows multiple stores per bucket
   ;; optional: use for S3-compatible services like Tigris or MinIO
   :endpoint-override {:protocol :https
                       :hostname "fly.storage.tigris.dev"}
   })

(def store (connect-s3-store s3-spec :opts {:sync? true}))

(k/assoc-in store ["foo" :bar] {:foo "baz"} {:sync? true})
(k/get-in store ["foo"] nil {:sync? true})
(k/exists? store "foo" {:sync? true})

(k/assoc-in store [:bar] 42 {:sync? true})
(k/update-in store [:bar] inc {:sync? true})
(k/get-in store [:bar] nil {:sync? true})
(k/dissoc store :bar {:sync? true})

(k/append store :error-log {:type :horrible} {:sync? true})
(k/log store :error-log {:sync? true})

(let [ba (byte-array (* 10 1024 1024) (byte 42))]
  (time (k/bassoc store "banana" ba {:sync? true})))

(k/bassoc store :binbar (byte-array (range 10)) {:sync? true})
(k/bget store :binbar (fn [{:keys [input-stream]}]
                        (map byte (slurp input-stream)))
       {:sync? true})

```

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
