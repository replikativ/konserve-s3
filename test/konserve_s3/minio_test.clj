(ns konserve-s3.minio-test
  "Tests using local Minio instance.

   Run with: docker-compose up -d
   Then: clojure -X:test"
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-s3.core :as s3]
            [konserve.core :as k]
            [konserve.store :as store])
  (:import [java.util UUID]))

;; Test store IDs - using stable UUIDs for reproducibility
(def sync-store-id #uuid "11111111-1111-1111-1111-111111111111")
(def async-store-id #uuid "22222222-2222-2222-2222-222222222222")
(def exists-store-id #uuid "33333333-3333-3333-3333-333333333333")
(def store1-id #uuid "44444444-4444-4444-4444-444444444444")
(def store2-id #uuid "55555555-5555-5555-5555-555555555555")

(def minio-spec
  {:region "us-east-1"
   :bucket "konserve-test"
   :id #uuid "66666666-6666-6666-6666-666666666666"
   :access-key "minioadmin"
   :secret "minioadmin"
   :path-style-access? true
   :endpoint-override {:protocol :http
                       :hostname "localhost"
                       :port 9000}})

(deftest minio-compliance-sync-test
  (testing "S3 compliance test with Minio (sync)"
    (let [spec (assoc minio-spec :backend :s3 :id sync-store-id)
          _     (store/delete-store spec {:sync? true})
          s     (store/create-store spec {:sync? true})]
      (compliance-test s)
      (store/release-store spec s {:sync? true})
      (store/delete-store spec {:sync? true}))))

(deftest minio-compliance-async-test
  (testing "S3 compliance test with Minio (async)"
    (let [spec (assoc minio-spec :backend :s3 :id async-store-id)
          _     (<!! (store/delete-store spec {:sync? false}))
          s     (<!! (store/create-store spec {:sync? false}))]
      (compliance-test s)
      (<!! (store/release-store spec s {:sync? false}))
      (<!! (store/delete-store spec {:sync? false})))))

(deftest minio-store-exists-test
  (testing "store-exists? with marker file"
    (let [spec (assoc minio-spec :backend :s3 :id exists-store-id)]
      ;; Clean up first
      (try (store/delete-store spec {:sync? true}) (catch Exception _))

      ;; Initially should not exist
      (is (false? (store/store-exists? spec {:sync? true})))

      ;; Create store - should write marker
      (let [s (store/create-store spec {:sync? true})]
        (is (some? s))
        (is (true? (store/store-exists? spec {:sync? true})))

        ;; Should error if we try to create again
        (is (thrown-with-msg? Exception #"already exists"
                              (store/create-store spec {:sync? true})))

        ;; Delete should remove marker
        (store/delete-store spec {:sync? true})
        (is (false? (store/store-exists? spec {:sync? true})))))))

(deftest minio-multi-store-test
  (testing "multiple stores in same bucket with different IDs"
    (let [spec1 (assoc minio-spec :backend :s3 :id store1-id)
          spec2 (assoc minio-spec :backend :s3 :id store2-id)]

      ;; Clean up
      (try (store/delete-store spec1 {:sync? true}) (catch Exception _))
      (try (store/delete-store spec2 {:sync? true}) (catch Exception _))

      ;; Create both stores
      (let [s1 (store/create-store spec1 {:sync? true})
            s2 (store/create-store spec2 {:sync? true})]

        (is (true? (store/store-exists? spec1 {:sync? true})))
        (is (true? (store/store-exists? spec2 {:sync? true})))

        ;; Write to each
        (k/assoc-in s1 [:key1] "value1" {:sync? true})
        (k/assoc-in s2 [:key2] "value2" {:sync? true})

        ;; Verify isolation
        (is (= "value1" (k/get-in s1 [:key1] nil {:sync? true})))
        (is (nil? (k/get-in s1 [:key2] nil {:sync? true})))

        (is (= "value2" (k/get-in s2 [:key2] nil {:sync? true})))
        (is (nil? (k/get-in s2 [:key1] nil {:sync? true})))

        ;; Clean up
        (store/release-store spec1 s1 {:sync? true})
        (store/release-store spec2 s2 {:sync? true})
        (store/delete-store spec1 {:sync? true})
        (store/delete-store spec2 {:sync? true})

        (is (false? (store/store-exists? spec1 {:sync? true})))
        (is (false? (store/store-exists? spec2 {:sync? true})))))))

(deftest minio-list-stores-test
  (testing "list-stores with registry"
    (let [spec1 (assoc minio-spec :backend :s3 :id store1-id)
          spec2 (assoc minio-spec :backend :s3 :id store2-id)
          minio-base (dissoc minio-spec :backend :id)]

      ;; Clean up
      (try (store/delete-store spec1 {:sync? true}) (catch Exception _))
      (try (store/delete-store spec2 {:sync? true}) (catch Exception _))

      ;; Initially no stores (or registry doesn't include our IDs)
      (let [initial-stores (s3/list-stores minio-base :opts {:sync? true})]
        (is (not (contains? initial-stores store1-id)))
        (is (not (contains? initial-stores store2-id))))

      ;; Create both stores
      (let [s1 (store/create-store spec1 {:sync? true})
            s2 (store/create-store spec2 {:sync? true})]

        ;; Should now appear in registry
        (let [stores (s3/list-stores minio-base :opts {:sync? true})]
          (is (contains? stores store1-id))
          (is (contains? stores store2-id)))

        ;; Clean up store 1
        (store/release-store spec1 s1 {:sync? true})
        (store/delete-store spec1 {:sync? true})

        ;; Registry should update
        (let [stores (s3/list-stores minio-base :opts {:sync? true})]
          (is (not (contains? stores store1-id)))
          (is (contains? stores store2-id)))

        ;; Clean up store 2
        (store/release-store spec2 s2 {:sync? true})
        (store/delete-store spec2 {:sync? true})

        ;; Both removed from registry
        (let [stores (s3/list-stores minio-base :opts {:sync? true})]
          (is (not (contains? stores store1-id)))
          (is (not (contains? stores store2-id))))))))

(deftest minio-optimistic-locking-concurrent-test
  (testing "Concurrent updates with optimistic locking - multiple store instances"
    (let [store-id (UUID/randomUUID)
          spec (assoc minio-spec
                      :backend :s3
                      :id store-id
                      :bucket "konserve-s3-optimistic-test"
                      :config {:optimistic-locking-retries 10})
          ;; Clean up first
          _ (try (store/delete-store spec {:sync? true}) (catch Exception _))
          ;; Create initial store to set up counter
          s-init (store/create-store spec {:sync? true})
          _ (k/assoc-in s-init [:counter] 0 {:sync? true})
          _ (store/release-store spec s-init {:sync? true})

          num-threads 5
          increments-per-thread 10
          expected-total (* num-threads increments-per-thread)

          ;; Track retry count and errors
          retry-count (atom 0)
          error-count (atom 0)
          success-count (atom 0)

          ;; Run concurrent updates - each thread connects to the existing store
          futures (doall
                   (for [thread-id (range num-threads)]
                     (future
                       ;; Each thread connects to the same S3 store (separate instance, same data)
                       (let [thread-store (store/connect-store spec {:sync? true})]
                         (try
                           (dotimes [i increments-per-thread]
                             (try
                               ;; Use truly synchronous mode so retry logic works with try/catch
                               (k/update-in thread-store [:counter] (fnil inc 0) {:sync? true})
                               (swap! success-count inc)
                               (catch Exception e
                                 (swap! error-count inc)
                                 (println "TEST CAUGHT EXCEPTION:" (.getMessage e) "type:" (:type (ex-data e)))
                                 (if (= :optimistic-lock-conflict (:type (ex-data e)))
                                   (do
                                     (swap! retry-count inc)
                                     (println "Thread" thread-id "hit optimistic lock conflict on iteration" i)
                                     ;; Retry logic already handles this in defaults.cljc
                                     ;; but if we get here, max retries was exceeded
                                     (throw e))
                                   (do
                                     (println "Thread" thread-id "error:" (.getMessage e) "type:" (:type (ex-data e)))
                                     (throw e))))))
                           (finally
                             (store/release-store spec thread-store {:sync? true})))))))]

      ;; Wait for all threads to complete
      (doseq [f futures]
        @f)

      ;; Verify final count
      (let [s-final (store/connect-store spec {:sync? true})
            final-count (k/get-in s-final [:counter] nil {:sync? true})]
        (println "Successful updates:" @success-count)
        (println "Errors encountered:" @error-count)
        (println "Optimistic lock conflicts (max retries exceeded):" @retry-count)
        (is (= expected-total final-count)
            (str "Expected " expected-total " but got " final-count))
        (store/release-store spec s-final {:sync? true}))

      ;; Clean up
      (store/delete-store spec {:sync? true}))))

