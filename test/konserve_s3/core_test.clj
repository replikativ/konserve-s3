(ns konserve-s3.core-test
  "Tests using local Minio instance or real AWS S3.

   For Minio (default):
   - Requires: docker-compose up -d
   - Runs against: http://localhost:9000 (minio)

   For real AWS (optional):
   - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
   - Modify s3-spec to remove :endpoint-override
   - Change :region to your desired AWS region
   - Update :bucket to a bucket you own

   Run with: clojure -M:test"
  (:require [clojure.test :refer [deftest testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.store :as store]
            [konserve-s3.core]
            [konserve.core :as k])
  (:import [java.util UUID]))

(def s3-spec
  "Default config uses Minio. To test against real AWS:
   1. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars
   2. Remove :endpoint-override line below
   3. Change :region to your AWS region
   4. Change :bucket to your AWS bucket"
  {:backend :s3
   :region "us-east-1"
   :bucket "konserve-test"
   :id (UUID/randomUUID)
   :access-key "minioadmin"
   :secret "minioadmin"
   :path-style-access? true
   :endpoint-override {:protocol :http
                       :hostname "localhost"
                       :port 9000}})

(deftest s3-compliance-sync-test
  (let [spec (assoc s3-spec :bucket "konserve-s3-sync-test")
        _     (store/delete-store spec {:sync? true})
        s     (store/create-store spec {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test s))
    (store/release-store spec s {:sync? true})
    (store/delete-store spec {:sync? true})))

(deftest s3-compliance-async-test
  (let [spec (assoc s3-spec :bucket "konserve-s3-async-test")
        _     (<!! (store/delete-store spec {:sync? false}))
        s     (<!! (store/create-store spec {:sync? false}))]
    (testing "Compliance test with asynchronous store"
      (compliance-test s))
    (<!! (store/release-store spec s {:sync? false}))
    (<!! (store/delete-store spec {:sync? false}))))

