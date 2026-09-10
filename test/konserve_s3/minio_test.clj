(ns konserve-s3.minio-test
  "Tests using local Minio instance.

   Run with: docker-compose up -d
   Then: clojure -X:test"
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test
                                              conditional-write-compliance-test]]
            [konserve-s3.core :as s3]
            [konserve-s3.storage :as storage]
            [konserve.impl.defaults :as defaults]
            [konserve.core :as k]
            [konserve.impl.storage-layout :as layout]
            [konserve.store :as store])
  (:import [java.util UUID]
           [software.amazon.awssdk.services.s3.model DeletedObject DeleteObjectsResponse S3Error]))

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

(def prefix-scope-a-id #uuid "88888888-8888-8888-8888-888888888888")
(def prefix-scope-b-id #uuid "99999999-9999-9999-9999-999999999999")

(deftest minio-keys-prefix-scoped-test
  (testing "-keys / -delete-store are scoped to their own store, not the bucket"
    ;; Regression: both listed the WHOLE bucket and filtered client-side, so
    ;; enumerating one store paged every object of every OTHER store into the
    ;; client — one ListObjectsV2 per 1000 bucket objects, on every call,
    ;; growing with each unrelated store. Reported from a bucket of 3.8M objects
    ;; holding ~1000 stores: ~3833 requests and ~1 GB pulled to enumerate a
    ;; 3465-object store, which timed out before reading a single value.
    (let [spec-a (assoc minio-spec :backend :s3 :id prefix-scope-a-id)
          spec-b (assoc minio-spec :backend :s3 :id prefix-scope-b-id)
          client (s3/s3-client minio-spec)
          bucket (:bucket minio-spec)
          prefix-a (storage/store-prefix (str prefix-scope-a-id))
          prefix-b (storage/store-prefix (str prefix-scope-b-id))]
      (try (store/delete-store spec-a {:sync? true}) (catch Exception _))
      (try (store/delete-store spec-b {:sync? true}) (catch Exception _))
      (let [sa (store/create-store spec-a {:sync? true})
            sb (store/create-store spec-b {:sync? true})]
        (try
          (doseq [i (range 3)]  (k/assoc sa (keyword (str "a" i)) i {:sync? true}))
          (doseq [i (range 25)] (k/assoc sb (keyword (str "b" i)) i {:sync? true}))

          (testing "S3 does the filtering: a prefixed listing returns one store's objects"
            (let [all   (s3/list-objects client bucket)
                  own-a (s3/list-objects client bucket prefix-a)]
              (is (= 4 (count own-a)) "3 blobs + marker")
              (is (every? #(.startsWith ^String % prefix-a) own-a))
              (is (> (count all) (+ (count own-a) 24))
                  "the bucket holds far more than store A — which is the point")))

          (testing "k/keys returns own keys, and the listing pulls only own objects"
            (let [r (s3/with-io-stats (k/keys sa {:sync? true}))]
              ;; :items is the load-bearing assertion here. The other three held
              ;; before the fix too — a whole-bucket listing is also a single
              ;; request below 1000 objects, and the client-side filter kept the
              ;; key set and the read count right while paying to transfer
              ;; everything. Only the objects the RESPONSE carried distinguishes
              ;; a scoped listing from a filtered one at this scale.
              (is (= 4 (get-in r [:stats :list :items] 0))
                  "the listing pulled 4 objects — A's own — not every object in the bucket (pre-fix: 30)")
              (is (= #{:a0 :a1 :a2} (into #{} (map :key) (:result r))))
              (is (= 1 (get-in r [:stats :list :n] 0))
                  "and it took one request")
              (is (= 3 (get-in r [:stats :get :n] 0))
                  "one metadata read per own key — B's 25 blobs are not read")))

          (testing "-delete-store deletes only its own objects"
            (store/release-store spec-a sa {:sync? true})
            ;; Measured around the deletion, not after it: every assertion below
            ;; lists BY prefix, so none of them can see what -delete-store itself
            ;; listed — and it is the second call site the prefix was added to.
            (let [r (s3/with-io-stats (store/delete-store spec-a {:sync? true}))]
              (is (= 4 (get-in r [:stats :list :items] 0))
                  "-delete-store listed A's 4 objects, not the whole bucket (pre-fix: 30)"))
            (is (empty? (s3/list-objects client bucket prefix-a)))
            (is (= 26 (count (s3/list-objects client bucket prefix-b)))
                "store B untouched: 25 blobs + marker")
            (is (= 25 (count (k/keys sb {:sync? true})))))
          (finally
            (try (store/release-store spec-b sb {:sync? true}) (catch Exception _))
            (try (store/delete-store spec-b {:sync? true}) (catch Exception _))
            (try (store/delete-store spec-a {:sync? true}) (catch Exception _))))))))

(deftest minio-delete-store-removes-the-cas-sidecar-test
  (testing "-delete-store removes konserve's fenced-write lock sidecar too"
    ;; konserve's `.cas` sidecar is PERMANENT and its `internal-artifact?`
    ;; docstring requires a backend that filters enumeration itself — this one —
    ;; to recognise it. `store-file?` did not, so the object would have survived
    ;; -delete-store: a store that reports itself deleted while one object per
    ;; fenced key remains. Neither backend can create one today (konserve only
    ;; takes the sidecar when the backing does not declare
    ;; PSelfConditionalWrite, and both do), so the object is seeded directly —
    ;; the point is that recognising it does not depend on that declaration
    ;; staying put.
    (let [store-id (UUID/randomUUID)
          spec     (assoc minio-spec :backend :s3 :id store-id
                          :bucket "konserve-s3-cas-sidecar-test")
          client   (s3/s3-client spec)
          bucket   (:bucket spec)
          prefix   (storage/store-prefix (str store-id))
          _        (try (store/delete-store spec {:sync? true}) (catch Exception _))
          s        (store/create-store spec {:sync? true})]
      (try
        (k/assoc s :fenced {:v 1} {:sync? true})
        (let [blob    (first (filter #(.endsWith ^String % ".ksv")
                                     (s3/list-objects client bucket prefix)))
              sidecar (str blob storage/cas-lock-suffix)]
          (s3/put-object client bucket sidecar (.getBytes "lock"))
          (is (some #{sidecar} (s3/list-objects client bucket prefix)) "seeded")
          (is (= #{:fenced} (into #{} (map :key) (k/keys s {:sync? true})))
              "the sidecar is konserve's bookkeeping, so it must not appear as a key")
          (store/release-store spec s {:sync? true})
          (store/delete-store spec {:sync? true})
          (is (empty? (s3/list-objects client bucket prefix))
              "the sidecar must not survive the store it belonged to"))
        (finally
          (try (store/release-store spec s {:sync? true}) (catch Exception _))
          (try (store/delete-store spec {:sync? true}) (catch Exception _))
          (doseq [k (s3/list-objects client bucket prefix)]
            (s3/delete client bucket k)))))))

(deftest minio-store-id-prefix-collision-test
  (testing "a store-id that is a prefix of another neither lists nor deletes its objects"
    ;; Two ways a sibling's objects reached this store's -keys, both regressions
    ;; and both live rather than inert — `->key` maps the leaked store-key back
    ;; onto the neighbour's real object, so it could be READ, and -delete-store
    ;; deleted it:
    ;;   `test2`  — the filters matched the bare store-id with no separator.
    ;;   `test_2` — NESTED under `test`'s `test_` prefix, so scoping the listing
    ;;              by prefix does not exclude it either (see storage/store-key).
    ;; store-id is (str (:id s3-spec)), i.e. any string, so neither needs an
    ;; unusual setup. UUID ids are structurally immune to both.
    (let [bucket  "konserve-s3-prefix-collision-test"
          client  (s3/s3-client (assoc minio-spec :bucket bucket))
          backing (fn [store-id] (s3/->S3Bucket client bucket store-id (atom {})))
          env     {:sync? true}
          uuid-a  "11111111-1111-1111-1111-111111111111"
          uuid-b  "22222222-2222-2222-2222-222222222222"]
      (when-not (s3/bucket-exists? client bucket)
        (s3/create-bucket client bucket))
      (doseq [sibling ["test2" "test_2"]]
        (testing (str "sibling store-id " (pr-str sibling))
          (doseq [k (s3/list-objects client bucket)]
            (s3/delete client bucket k))
          (s3/put-object client bucket (str "test_" uuid-a ".ksv")           (.getBytes "a"))
          (s3/put-object client bucket "test_.konserve-metadata"             (.getBytes "konserve"))
          (s3/put-object client bucket (str sibling "_" uuid-b ".ksv")       (.getBytes "b"))
          (s3/put-object client bucket (str sibling "_.konserve-metadata")   (.getBytes "konserve"))

          (is (= #{(str uuid-a ".ksv")} (set (layout/-keys (backing "test") env)))
              "`test` lists its own blob only")
          (is (= #{(str uuid-b ".ksv")} (set (layout/-keys (backing sibling) env)))
              "and the sibling lists its own")

          (layout/-delete-store (backing "test") env)
          (is (= #{(str sibling "_" uuid-b ".ksv") (str sibling "_.konserve-metadata")}
                 (set (s3/list-objects client bucket)))
              "deleting `test` left the sibling's objects intact")

          (layout/-delete-store (backing sibling) env)
          (is (empty? (s3/list-objects client bucket))))))))

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

(deftest minio-fenced-write-does-not-adopt-a-foreign-etag-test
  (testing "an accepted :expected-revision write cannot overwrite an intervening replacement

            Regression, reported against konserve-s3 0.1.42 / konserve 0.9.391 on
            SeaweedFS and reproduced here on MinIO — the mechanism is client-side,
            not provider-specific.

            konserve's `update-blob*` writes through a blob it creates itself, so
            the blob that read the object is not the blob that PUTs it, and the
            If-Match token has to travel through this backing's store-wide
            etag-cache. `-read-header` used to publish into that cache on EVERY
            read. So a `keys` listing — which reads every blob in the store —
            landing between a fenced write's revision check and its PUT replaced
            that write's precondition with the ETag of the value it was about to
            clobber. The stale write then satisfied If-Match, reported success, and
            the newer value was lost. Without the listing the same write was
            correctly rejected, so the guarantee held or not depending on unrelated
            traffic on the same handle.

            The schedule is made deterministic by pausing the writer inside
            konserve's check-revision! — the same hook the reporter used — rather
            than by racing threads."
    (let [spec (assoc minio-spec :backend :s3 :id (UUID/randomUUID)
                      :bucket "konserve-s3-fence-etag-test")
          _    (try (store/delete-store spec {:sync? true}) (catch Exception _))
          a    (store/create-store spec {:sync? true})
          b    (store/connect-store spec {:sync? true})
          ;; Run `body` once, immediately after the next successful revision check.
          after-revision-check
          (fn [body f]
            (let [orig  defaults/check-revision!
                  fired (atom false)]
              (with-redefs [defaults/check-revision!
                            (fn [& args]
                              (apply orig args)
                              (when (compare-and-set! fired false true) (body)))]
                (f))))
          stale-write!
          (fn [kk rev]
            (try (k/assoc a kk {:generation :stale-writer}
                          {:sync? true :expected-revision rev})
                 :accepted
                 (catch Exception e (:type (ex-data e)))))]
      (try
        (testing "a listing between the check and the PUT does not refresh the precondition"
          (k/assoc a :fenced {:generation 1} {:sync? true})
          (let [rev (k/revision a :fenced {:sync? true})
                outcome (after-revision-check
                         (fn []
                           ;; B replaces the value, then A lists keys — the listing
                           ;; reads B's new object through A's shared etag-cache.
                           (k/assoc b :fenced {:generation 2} {:sync? true})
                           (doall (k/keys a {:sync? true})))
                         #(stale-write! :fenced rev))]
            (is (= :konserve/revision-mismatch outcome)
                "the stale fenced write must be rejected")
            (is (= {:generation 2} (k/get b :fenced nil {:sync? true}))
                "and B's newer value must survive")))

        (testing "control: same schedule without the listing was always rejected"
          (k/assoc a :control {:generation 1} {:sync? true})
          (let [rev (k/revision a :control {:sync? true})
                outcome (after-revision-check
                         (fn [] (k/assoc b :control {:generation 2} {:sync? true}))
                         #(stale-write! :control rev))]
            (is (= :konserve/revision-mismatch outcome))
            (is (= {:generation 2} (k/get b :control nil {:sync? true})))))

        (testing "an UNCONTENDED fenced write still succeeds across a listing"
          ;; The gate must reject only foreign tokens, not the operation's own.
          (k/assoc a :quiet {:generation 1} {:sync? true})
          (let [rev (k/revision a :quiet {:sync? true})
                outcome (after-revision-check
                         (fn [] (doall (k/keys a {:sync? true})))
                         #(try (k/assoc a :quiet {:generation 2}
                                        {:sync? true :expected-revision rev})
                               :accepted
                               (catch Exception e (:type (ex-data e)))))]
            (is (= :accepted outcome))
            (is (= {:generation 2} (k/get a :quiet nil {:sync? true})))))
        (finally
          (try (store/release-store spec a {:sync? true}) (catch Exception _))
          (try (store/release-store spec b {:sync? true}) (catch Exception _))
          (try (store/delete-store spec {:sync? true}) (catch Exception _)))))))
(deftest minio-in-place-is-forced-test
  (testing "{:in-place? false} is overridden: S3 is in-place only"
    ;; Rename mode on S3 is CopyObject + DeleteObject per write — two extra
    ;; requests, no atomicity gained (a PUT already replaces atomically) — and it
    ;; makes fencing impossible, since the If-Match token belongs to the target's
    ;; key rather than the `.new` one a rename-mode write goes through. So a
    ;; caller who set it got a slower store with :expected-revision silently
    ;; refused. Now the setting is ignored (with a warning) and fencing works.
    (let [spec (assoc minio-spec :backend :s3 :id (UUID/randomUUID)
                      :config {:in-place? false})
          _    (try (store/delete-store spec {:sync? true}) (catch Exception _))
          s    (store/create-store spec {:sync? true})]
      (try
        (is (true? (get-in s [:config :in-place?]))
            "the connected store runs in-place regardless")
        (k/assoc s :k {:v 1} {:sync? true})
        (let [rev (k/revision s :k {:sync? true})]
          (k/assoc s :k {:v 2} {:sync? true :expected-revision rev})
          (is (= {:v 2} (k/get s :k nil {:sync? true}))
              "a fenced write succeeds — it would have been refused in rename mode"))
        (finally
          (store/release-store spec s {:sync? true})
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

(defn- ^DeleteObjectsResponse delete-response
  "A DeleteObjectsResponse with the given deleted keys and per-key errors."
  [deleted-keys errors]
  (-> (DeleteObjectsResponse/builder)
      (.deleted ^java.util.Collection
       (mapv (fn [k] (-> (DeletedObject/builder) (.key k) (.build))) deleted-keys))
      (.errors ^java.util.Collection
       (mapv (fn [[k code msg]]
               (-> (S3Error/builder) (.key k) (.code code) (.message msg) (.build)))
             errors))
      (.build)))

(deftest batch-delete-raises-on-per-key-failure-test
  (testing "a partial batch delete must RAISE, not report success.

            `DeleteObjects` returns per-key failures in the response body and
            does NOT throw, so discarding the response made a partial delete
            indistinguishable from a complete one -- on the tenant-offboarding
            and erasure path, where a caller has to be able to tell whether the
            erasure happened.

            Driven by constructing the response rather than by provoking S3:
            neither S3 nor MinIO will fail a delete on request (both treat
            deleting an absent key as success), so an end-to-end test can only
            reach the path where nothing goes wrong -- the branch that least
            needs covering."
    (testing "an all-succeeded batch passes the response through"
      (let [resp (delete-response ["a" "b"] [])]
        (is (identical? resp (s3/check-delete-response! resp "bucket")))))

    (testing "a per-key failure raises, naming what survived"
      (let [resp (delete-response ["a"] [["b" "AccessDenied" "Access Denied"]
                                         ["c" "InternalError" "boom"]])
            e    (try (s3/check-delete-response! resp "bucket") nil
                      (catch clojure.lang.ExceptionInfo e e))
            d    (ex-data e)]
        (is (some? e) "a partial delete must not return normally")
        (is (= :konserve.s3/batch-delete-incomplete (:type d)))
        (is (= "bucket" (:bucket d)))
        (is (= 1 (:deleted d)) "reports how many actually went")
        (is (= [{:key "b" :code "AccessDenied" :message "Access Denied"}
                {:key "c" :code "InternalError" :message "boom"}]
               (:failed d))
            "and names every key that survived, with its S3 error code")))))

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
              ;; This store's own objects. Listing the whole bucket here took
              ;; whatever sorted first across every store in it, so an aborted
              ;; earlier run could make this delete a FOREIGN object and still
              ;; pass — the same unscoped listing this suite now fixes elsewhere.
              own    (s3/list-objects client bucket
                                      (storage/store-prefix (str (:id spec))))
              resp   (s3/delete-keys client bucket (take 1 own))]
          (is (= 1 (count (.deleted resp)))
              "the response is inspected, and reports what it deleted")
          (is (empty? (seq (.errors resp)))
              "a delete every key of which succeeded raises nothing"))
        (finally
          (store/release-store spec st {:sync? true})
          (try (store/delete-store spec {:sync? true}) (catch Exception _ nil)))))))
