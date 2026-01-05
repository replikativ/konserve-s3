(ns konserve-s3.minio-test
  "Tests using local Minio instance.

   Run with: docker-compose up -d
   Then: clojure -X:test"
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-s3.core :as s3]
            [konserve.core :as k]
            [konserve.store :as store]))

(def minio-spec
  {:region "us-east-1"
   :bucket "konserve-test"
   :store-id "test-store"
   :access-key "minioadmin"
   :secret "minioadmin"
   :path-style-access? true
   :endpoint-override {:protocol :http
                       :hostname "localhost"
                       :port 9000}})

(deftest minio-compliance-sync-test
  (testing "S3 compliance test with Minio (sync)"
    (let [spec (assoc minio-spec :store-id "sync-compliance-test")
          _     (s3/delete-store spec :opts {:sync? true})
          store (s3/connect-store spec :opts {:sync? true})]
      (compliance-test store)
      (s3/release store {:sync? true})
      (s3/delete-store spec :opts {:sync? true}))))

(deftest minio-compliance-async-test
  (testing "S3 compliance test with Minio (async)"
    (let [spec (assoc minio-spec :store-id "async-compliance-test")
          _     (<!! (s3/delete-store spec :opts {:sync? false}))
          store (<!! (s3/connect-store spec :opts {:sync? false}))]
      (compliance-test store)
      (<!! (s3/release store {:sync? false}))
      (<!! (s3/delete-store spec :opts {:sync? false})))))

(deftest minio-store-exists-test
  (testing "store-exists? with marker file"
    (let [spec (assoc minio-spec :backend :s3 :store-id "exists-test" :opts {:sync? true})]
      ;; Clean up first
      (try (store/delete-store spec) (catch Exception _))

      ;; Initially should not exist
      (is (false? (store/store-exists? spec)))

      ;; Create store - should write marker
      (let [s (store/create-store spec)]
        (is (some? s))
        (is (true? (store/store-exists? spec)))

        ;; Should error if we try to create again
        (is (thrown-with-msg? Exception #"already exists"
                              (store/create-store spec)))

        ;; Delete should remove marker
        (store/delete-store spec)
        (is (false? (store/store-exists? spec)))))))

(deftest minio-multi-store-test
  (testing "multiple stores in same bucket with different store-ids"
    (let [spec1 (assoc minio-spec :backend :s3 :store-id "store1" :opts {:sync? true})
          spec2 (assoc minio-spec :backend :s3 :store-id "store2" :opts {:sync? true})]

      ;; Clean up
      (try (store/delete-store spec1) (catch Exception _))
      (try (store/delete-store spec2) (catch Exception _))

      ;; Create both stores
      (let [s1 (store/create-store spec1)
            s2 (store/create-store spec2)]

        (is (true? (store/store-exists? spec1)))
        (is (true? (store/store-exists? spec2)))

        ;; Write to each
        (k/assoc-in s1 [:key1] "value1" {:sync? true})
        (k/assoc-in s2 [:key2] "value2" {:sync? true})

        ;; Verify isolation
        (is (= "value1" (k/get-in s1 [:key1] nil {:sync? true})))
        (is (nil? (k/get-in s1 [:key2] nil {:sync? true})))

        (is (= "value2" (k/get-in s2 [:key2] nil {:sync? true})))
        (is (nil? (k/get-in s2 [:key1] nil {:sync? true})))

        ;; Clean up
        (store/delete-store spec1)
        (store/delete-store spec2)

        (is (false? (store/store-exists? spec1)))
        (is (false? (store/store-exists? spec2)))))))
