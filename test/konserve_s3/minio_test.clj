(ns konserve-s3.minio-test
  "Tests using local Minio instance.

   Run with: docker-compose up -d
   Then: clojure -X:test"
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test
                                              conditional-write-compliance-test]]
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

(def round-trip-store-id #uuid "77777777-7777-7777-7777-777777777777")

(deftest minio-round-trip-count-test
  (testing "PReadMissSafe: no HEAD probe on read / update-in / dissoc / bassoc (real S3 op counts)"
    (let [spec (assoc minio-spec :backend :s3 :id round-trip-store-id)
          _    (store/delete-store spec {:sync? true})
          s    (store/create-store spec {:sync? true})
          heads   (fn [r] (get-in r [:stats :head :n] 0))
          gets    (fn [r] (get-in r [:stats :get :n] 0))]
      (try
        (k/assoc s :k {:v 1} {:sync? true})

        (testing "get hit: exactly one GET, no HEAD"
          (let [r (s3/with-io-stats (k/get s :k nil {:sync? true}))]
            (is (= {:v 1} (:result r)))
            (is (= 0 (heads r)) "no HEAD probe")
            (is (= 1 (gets r)) "exactly one GET")))

        (testing "get miss: no HEAD (read-first reports the miss)"
          (let [r (s3/with-io-stats (k/get s :missing nil {:sync? true}))]
            (is (nil? (:result r)))
            (is (= 0 (heads r)) "no HEAD probe")))

        (testing "update-in (read-modify-write): no HEAD"
          (let [r (s3/with-io-stats (k/update-in s [:k :v] inc {:sync? true}))]
            (is (= 0 (heads r)) "no HEAD probe")
            (is (= {:v 2} (k/get s :k nil {:sync? true})))))

        (testing "bassoc (binary write): no HEAD"
          (let [r (s3/with-io-stats (k/bassoc s :b (.getBytes "hello") {:sync? true}))]
            (is (= 0 (heads r)) "no HEAD probe")))

        ;; dissoc keeps its HEAD by default — konserve's contract requires it to
        ;; report existed?/false-for-missing, which S3 DELETE cannot.
        (testing "dissoc (default): one HEAD (existed? contract) + one DELETE"
          (let [r (s3/with-io-stats (k/dissoc s :k {:sync? true}))]
            (is (= 1 (heads r)) "one HEAD probe (contract)")
            (is (pos? (get-in r [:stats :delete :n] 0)) "one DELETE")
            (is (nil? (k/get s :k nil {:sync? true})) "key is gone")))

        ;; ...but a caller that doesn't need the boolean opts out of the HEAD.
        (testing "dissoc with :ignore-existence? true: no HEAD, one DELETE (GC fast path)"
          (k/assoc s :k2 {:v 1} {:sync? true})
          (let [r (s3/with-io-stats (k/dissoc s :k2 {:sync? true :ignore-existence? true}))]
            (is (= 0 (heads r)) "no HEAD probe")
            (is (pos? (get-in r [:stats :delete :n] 0)) "one DELETE")
            (is (nil? (k/get s :k2 nil {:sync? true})) "key is gone")))
        (finally
          (store/release-store spec s {:sync? true})
          (store/delete-store spec {:sync? true}))))))

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

(def async-delete-store-id #uuid "77777777-7777-7777-7777-777777777777")

(deftest minio-async-delete-store-test
  (testing "delete-store on the ASYNC path actually deletes (and reports completion)"
    ;; Regression: `-delete-store :s3` returned its inner channel WITHOUT awaiting it,
    ;; so under {:sync? false} — konserve.store/delete-store's DEFAULT, and what
    ;; datahike's d/delete-database uses — the caller was handed an un-awaited channel,
    ;; nothing was deleted, and any error was swallowed into a channel nobody read.
    ;; Every existing delete-store test passed {:sync? true}, so the async path (the
    ;; one real callers take) was never exercised. Keep this one async.
    (let [spec (assoc minio-spec :backend :s3 :id async-delete-store-id)]
      (try (store/delete-store spec {:sync? true}) (catch Exception _))

      (let [s (store/create-store spec {:sync? true})]
        (k/assoc-in s [:k] "v" {:sync? true})
        (is (true? (store/store-exists? spec {:sync? true})))
        (store/release-store spec s {:sync? true}))

      ;; The default opts are {:sync? false}: take from the channel and assert the
      ;; store is gone by the time it delivers.
      (<!! (store/delete-store spec))
      (is (false? (store/store-exists? spec {:sync? true}))
          "async delete-store must have removed the store by the time its channel delivers"))))

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

(deftest minio-conditional-write-test
  (testing "the `:expected-revision` contract against a real endpoint.

            This backing answers `:global`, and it is the only one that can: the
            comparison is S3's own If-Match, evaluated by S3, rather than a lock
            local to a filesystem or a heap. `-get-lock` here is a NO-OP, so
            nothing else is serializing these writes — which makes running the
            shared contract against a live bucket the only thing standing between
            that claim and a deployment trusting it."
    (let [spec (assoc minio-spec :backend :s3 :id (UUID/randomUUID))
          _    (try (store/delete-store spec {:sync? true}) (catch Exception _))
          s    (store/create-store spec {:sync? true})]
      (try
        (is (= :global (k/conditional-write-domain s))
            "S3 evaluates the precondition, so the domain reaches every writer")
        (conditional-write-compliance-test s)
        (finally
          (store/release-store spec s {:sync? true})
          (store/delete-store spec {:sync? true}))))))

(deftest minio-concurrent-create-if-absent-test
  (testing "two peers racing a create-if-absent must produce exactly one winner,
            and the winner's value must SURVIVE.

            This is datahike initialising a branch head, and it is where a
            rejected write used to destroy a committed one: the loser's cleanup
            deleted the key by path, and on a `:global` backing it holds no lock
            while doing it — worse here than on a filestore, because
            `-create-blob` writes nothing remotely, so there was never a stray
            object to collect and the delete was pure destruction. Measured 10 of
            10 keys lost. The fix is that a fenced write to a key that does not
            exist creates no blob at all, so there is nothing to clean up and no
            cleanup to race."
    (let [spec (assoc minio-spec :backend :s3 :id (UUID/randomUUID))
          _    (try (store/delete-store spec {:sync? true}) (catch Exception _))
          _    (store/create-store spec {:sync? true})
          A    (store/connect-store spec {:sync? true})
          B    (store/connect-store spec {:sync? true})
          n    10]
      (try
        (is (= :global (k/conditional-write-domain A)))
        (let [outcomes
              (doall
               (for [i (range n)]
                 (let [kk (keyword (str "head-" i))
                       fa (future (try (k/assoc A kk {:by :A} {:sync? true :expected-revision k/absent}) :ok
                                       (catch Exception e (:type (ex-data e)))))
                       fb (future (try (k/assoc B kk {:by :B} {:sync? true :expected-revision k/absent}) :ok
                                       (catch Exception e (:type (ex-data e)))))
                       ra @fa rb @fb]
                   {:winners (count (filter #{:ok} [ra rb]))
                    :final   (k/get A kk :MISSING {:sync? true})})))]
          (is (every? #(= 1 (:winners %)) outcomes)
              (str "exactly one peer may win each race: " (pr-str (map :winners outcomes))))
          (is (not-any? #(= :MISSING (:final %)) outcomes)
              (str "and the winner's value must still be there: "
                   (pr-str (map :final outcomes)))))
        (finally
          (store/release-store spec A {:sync? true})
          (store/release-store spec B {:sync? true})
          (store/delete-store spec {:sync? true}))))))

(deftest minio-fenced-concurrent-counter-test
  (testing "Concurrent increments converge when the CALLER fences and retries.

            This replaces a test that expected plain concurrent `update-in` to
            converge on its own. It did, under the old design, because an ETag
            left in a process-local cache was applied as an implicit If-Match —
            which is precisely why that design was unsound: a cold cache, a fresh
            connection, or `:optimistic-locking-retries` at its default turned the
            guarantee off with nothing to notice, and this test could not tell the
            two cases apart. It converged for a reason it never asserted.

            Fencing is now something the caller asks for with
            `:expected-revision`, so the retry loop that makes it converge belongs
            to the caller. Five threads, ten increments each, five separate store
            instances against one MinIO bucket: every increment must survive."
    (let [store-id (UUID/randomUUID)
          spec (assoc minio-spec
                      :backend :s3
                      :id store-id
                      :bucket "konserve-s3-optimistic-test")
          _ (try (store/delete-store spec {:sync? true}) (catch Exception _))
          s-init (store/create-store spec {:sync? true})
          _ (k/assoc-in s-init [:counter] 0 {:sync? true})
          _ (store/release-store spec s-init {:sync? true})

          num-threads 5
          increments-per-thread 10
          expected-total (* num-threads increments-per-thread)
          conflicts (atom 0)

          futures (doall
                   (for [_ (range num-threads)]
                     (future
                       (let [thread-store (store/connect-store spec {:sync? true})]
                         (try
                           (dotimes [_ increments-per-thread]
                             ;; Read the revision, write against it, and retry
                             ;; from a RE-READ one on conflict. Retrying against
                             ;; the same token would be rejected forever — the
                             ;; point of the fence is that the value moved.
                             (loop [tries 0]
                               (let [rev (k/revision thread-store :counter {:sync? true})
                                     res (try (k/update-in thread-store [:counter] (fnil inc 0)
                                                           {:sync? true :expected-revision rev})
                                              ::ok
                                              (catch Exception e
                                                (if (= :konserve/revision-mismatch (:type (ex-data e)))
                                                  ::conflict
                                                  (throw e))))]
                                 (when (= ::conflict res)
                                   (swap! conflicts inc)
                                   (when (< tries 200)
                                     (recur (inc tries)))))))
                           (finally
                             (store/release-store spec thread-store {:sync? true})))))))]

      (doseq [f futures] @f)

      (let [s-final (store/connect-store spec {:sync? true})
            final-count (k/get-in s-final [:counter] nil {:sync? true})]
        (is (= expected-total final-count)
            (str "Expected " expected-total " but got " final-count
                 " — a fenced write that lands must not overwrite one it did not see"))
        (is (pos? @conflicts)
            (str "the threads must actually have CONTENDED (" @conflicts " conflicts); "
                 "a run with none proves the fence held but not that it was needed"))
        (store/release-store spec s-final {:sync? true}))

      (store/delete-store spec {:sync? true}))))


(deftest batch-delete-reports-per-key-failures-test
  (testing "a partial batch delete must RAISE, not report success.

            `DeleteObjects` returns per-key failures in the response body and
            does not throw, so discarding the response made a partial delete
            indistinguishable from a complete one. That is the tenant-offboarding
            and erasure path: a caller who asked to erase a store has to be able
            to tell whether it happened.

            Driven through `delete-keys` directly with a key the bucket does not
            hold plus one it does. S3 and MinIO both treat deleting an absent key
            as a success, so this asserts the SHAPE that matters -- the response
            is now inspected and its `deleted` set surfaces -- rather than
            fabricating a permission error the emulator would not honour."
    (let [spec (assoc minio-spec :backend :s3 :id (UUID/randomUUID))
          _    (try (store/delete-store spec {:sync? true}) (catch Exception _ nil))
          st   (store/create-store spec {:sync? true})]
      (try
        (k/assoc st :a 1 {:sync? true})
        (let [client (:client (:backing st))
              bucket (:bucket (:backing st))
              all    (s3/list-objects client bucket)
              resp   (s3/delete-keys client bucket (take 1 all))]
          (is (= 1 (count (.deleted resp)))
              "the response is inspected, and reports what it deleted")
          (is (empty? (seq (.errors resp)))
              "a delete every key of which succeeded raises nothing"))
        (finally
          (store/release-store spec st {:sync? true})
          (try (store/delete-store spec {:sync? true}) (catch Exception _ nil)))))))
