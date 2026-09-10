(ns konserve-s3.minio-test
  "MinIO/S3 integration tests mirroring the JVM konserve-s3.minio-test,
   adapted to the async-only cljs backend. They cover the paths the async
   compliance suite does not: the store lifecycle
   (store-exists? / \"already exists\"), multi-store isolation in one bucket,
   store discovery via list-stores, and optimistic locking under concurrency.
   Going through konserve.store also exercises the `:s3` multimethod dispatch
   layer, which connect-s3-store bypasses.

   Network test — like compliance-test it needs a reachable bucket + credentials
   supplied via env vars, falling back to the docker-compose MinIO at
   localhost:9000:

       S3_ENDPOINT    e.g. http://localhost:9000 (MinIO)
       S3_BUCKET      bucket name (must already exist)
       S3_ACCESS_KEY  / S3_SECRET   credentials
       S3_REGION      e.g. us-east-1 / auto
       S3_PATH_STYLE  \"false\" for Amazon virtual-hosted addressing (default true)

   Each test uses fresh random store ids and cleans up in a finally, so
   aborted/parallel runs cannot collide. Node-only (reads process.env); the
   shadow browser/karma builds exclude it (see shadow-cljs.edn)."
  (:require [clojure.core.async :refer [go <!] :include-macros true]
            [cljs.test :refer [deftest is testing async]]
            [konserve.core :as k]
            [konserve.store :as store]
            [konserve.impl.storage-layout :as storage-layout]
            [konserve.impl.defaults :as defaults]
            [konserve-s3.storage :as storage]
            [konserve-s3.core :as s3]))

(defn- env [k]
  (some-> (.-env js/process) (aget k)))

(defn- base-spec []
  {:backend     :s3
   :endpoint    (or (env "S3_ENDPOINT") "http://localhost:9000")
   :bucket      (or (env "S3_BUCKET") "konserve-test")
   :access-key  (or (env "S3_ACCESS_KEY") "minioadmin")
   :secret      (or (env "S3_SECRET") "minioadmin")
   :region      (or (env "S3_REGION") "us-east-1")
   :path-style? (not= "false" (env "S3_PATH_STYLE"))})

(defn- spec
  "A backend config for a fresh, random store id in the shared test bucket."
  []
  (assoc (base-spec) :id (random-uuid)))

(def ^:private opts {:sync? false})

(deftest ^:slow store-lifecycle-test
  (testing "store-exists? tracks create/delete and create refuses to clobber"
    (async done
           (let [s (spec)]
             (go
               (try
                 (is (false? (<! (store/store-exists? s opts)))
                     "absent before create")
                 (let [store (<! (store/create-store s opts))]
                   (is (some? store) "create yields a store")
                   (is (true? (<! (store/store-exists? s opts)))
                       "present after create")
                   ;; Creating the same store again must error, not clobber.
                   (let [res (<! (store/create-store s opts))]
                     (is (and (instance? js/Error res)
                              (re-find #"already exists" (.-message res)))
                         "second create errors with already-exists"))
                   (<! (store/release-store s store opts))
                   (<! (store/delete-store s opts))
                   (is (false? (<! (store/store-exists? s opts)))
                       "absent after delete"))
                 (catch :default e
                   (is false (str "store-lifecycle-test threw: " (.-message e))))
                 (finally
                   (<! (store/delete-store s opts))
                   (done))))))))

(deftest ^:slow multi-store-isolation-test
  (testing "two stores in one bucket keep their keyspaces separate"
    (async done
           (let [s1 (spec)
                 s2 (spec)]
             (go
               (try
                 (let [store1 (<! (store/create-store s1 opts))
                       store2 (<! (store/create-store s2 opts))]
                   (is (true? (<! (store/store-exists? s1 opts))))
                   (is (true? (<! (store/store-exists? s2 opts))))

                   (<! (k/assoc-in store1 [:key1] "value1" opts))
                   (<! (k/assoc-in store2 [:key2] "value2" opts))

                   (is (= "value1" (<! (k/get-in store1 [:key1] nil opts))))
                   (is (nil?       (<! (k/get-in store1 [:key2] nil opts))))
                   (is (= "value2" (<! (k/get-in store2 [:key2] nil opts))))
                   (is (nil?       (<! (k/get-in store2 [:key1] nil opts))))

                   (<! (store/release-store s1 store1 opts))
                   (<! (store/release-store s2 store2 opts)))
                 (catch :default e
                   (is false (str "multi-store-isolation-test threw: " (.-message e))))
                 (finally
                   (<! (store/delete-store s1 opts))
                   (<! (store/delete-store s2 opts))
                   (done))))))))

(deftest ^:slow nested-store-id-isolation-test
  (testing "a store-id nested under another's prefix is neither listed nor deleted by it"
    ;; Regression (shared with core.clj): -keys/-delete-store listed under the
    ;; BARE store-id, so `p`'s listing also returned `p2`'s and `p_2`'s objects —
    ;; and the store-key it yielded mapped straight back onto the neighbour's
    ;; real object, so it could be read and -delete-store deleted it.
    ;; Driven at the backing-store level: konserve.store enforces a UUID :id
    ;; (UUIDs are structurally immune), but the backend's own connect-s3-store
    ;; takes (str (:id s3-spec)) — any string.
    (async done
           (let [conn   (s3/connect (base-spec))
                 tag    (str "nest" (rand-int 1e9))
                 body   (.encode (js/TextEncoder.) "x")
                 uuid-a "11111111-1111-1111-1111-111111111111"
                 uuid-b "22222222-2222-2222-2222-222222222222"
                 backing (fn [store-id] (s3/->S3BackingStore conn store-id (atom {})))]
             (go
               (try
                 (doseq [sibling [(str tag "2") (str tag "_2")]]
                   (<! (s3/put-object conn (str tag "_" uuid-a ".ksv") body))
                   (<! (s3/put-object conn (str tag "_.konserve-metadata") body))
                   (<! (s3/put-object conn (str sibling "_" uuid-b ".ksv") body))
                   (<! (s3/put-object conn (str sibling "_.konserve-metadata") body))

                   (is (= #{(str uuid-a ".ksv")}
                          (set (<! (storage-layout/-keys (backing tag) opts))))
                       (str "outer store lists its own blob only (sibling " sibling ")"))
                   (is (= #{(str uuid-b ".ksv")}
                          (set (<! (storage-layout/-keys (backing sibling) opts))))
                       "and the sibling lists its own")

                   (<! (storage-layout/-delete-store (backing tag) opts))
                   (is (= #{(str sibling "_" uuid-b ".ksv") (str sibling "_.konserve-metadata")}
                          (set (<! (s3/list-objects conn sibling))))
                       "deleting the outer store left the sibling's objects intact")
                   (<! (storage-layout/-delete-store (backing sibling) opts))
                   (is (empty? (<! (s3/list-objects conn (str tag))))
                       "and both stores are gone afterwards"))
                 (catch :default e
                   (is false (str "nested-store-id-isolation-test threw: " (.-message e))))
                 (finally
                   (doseq [k (<! (s3/list-objects conn tag))]
                     (<! (s3/delete-object conn k)))
                   (done))))))))

(deftest ^:slow fenced-write-consumes-only-its-own-etag-test
  (testing "the etag-cache carries a fence token only for the operation that read it"
    ;; Regression (reported on the JVM, same design here): -read-header published
    ;; the ETag into the store-wide cache on EVERY read, and -sync consumed
    ;; whatever it found. A `keys` listing — which reads every blob in the store —
    ;; between a fenced write's revision check and its PUT therefore replaced that
    ;; write's precondition with the ETag of the value it was about to clobber, so
    ;; the stale write satisfied If-Match and the newer value was lost.
    ;; Driven at the backing-store level: the cljs store is async-only, so there is
    ;; no way to pause a real operation mid-flight, but publication and
    ;; consumption are exactly what the fix changes.
    (async done
           (let [conn      (s3/connect (base-spec))
                 store-id  (str (random-uuid))
                 cache     (atom {})
                 backing   (s3/->S3BackingStore conn store-id cache)
                 store-key (defaults/key->store-key :fenced)
                 object    (storage/->key store-id store-key)
                 opts      {:sync? false}
                 bytes     (fn [] (js/Uint8Array. #js [1 2 3]))]
             (go
               (try
                 ;; Seed an object to read (content is irrelevant: only its ETag is).
                 (<! (s3/put-object conn object (bytes)))

                 (testing "an unfenced read publishes nothing"
                   (let [blob (<! (storage-layout/-create-blob backing store-key opts))]
                     (<! (storage-layout/-read-header blob opts))
                     (is (empty? @cache)
                         "a plain read — a `keys` listing reads every blob this way — must not publish a token")))

                 (testing "a fenced read publishes a token tagged with its revision"
                   (let [blob (<! (storage-layout/-create-blob backing store-key
                                                               (assoc opts :expected-revision 41)))]
                     (<! (storage-layout/-read-header blob (assoc opts :expected-revision 41)))
                     (is (= 41 (:for-revision (get @cache object))))
                     (is (string? (:etag (get @cache object))))))

                 (testing "-sync refuses a token published for another revision"
                   ;; What a foreign read (or an intervening listing, before the fix)
                   ;; would have left behind.
                   (reset! cache {object {:etag "\"someone-elses-etag\"" :for-revision 99}})
                   (let [env  (assoc opts :expected-revision 41)
                         blob (<! (storage-layout/-create-blob backing store-key env))]
                     (<! (storage-layout/-write-header blob (bytes) env))
                     (<! (storage-layout/-write-meta blob (bytes) env))
                     (<! (storage-layout/-write-value blob (bytes) 3 env))
                     (let [res (<! (storage-layout/-sync blob env))]
                       (is (= :konserve/conditional-write-unsupported (:type (ex-data res)))
                           "it must refuse rather than fence against an object it never read"))))

                 (catch :default e
                   (is false (str "fenced-write-consumes-only-its-own-etag-test threw: " (.-message e))))
                 (finally
                   (<! (s3/delete-object conn object))
                   (done))))))))

(deftest ^:slow in-place-is-forced-test
  (testing "{:in-place? false} is overridden: S3 is in-place only (mirrors core.clj)"
    (async done
           (let [s (assoc (spec) :config {:in-place? false})]
             (go
               (try
                 (let [st (<! (store/create-store s opts))]
                   (is (true? (get-in st [:config :in-place?])))
                   (<! (k/assoc st :k {:v 1} opts))
                   (let [rev (<! (k/revision st :k opts))
                         res (<! (k/assoc st :k {:v 2} (assoc opts :expected-revision rev)))]
                     (is (not (instance? js/Error res))
                         "a fenced write succeeds — rename mode could only refuse it")
                     (is (= {:v 2} (<! (k/get st :k nil opts)))))
                   (<! (store/release-store s st opts)))
                 (catch :default e
                   (is false (str "in-place-is-forced-test threw: " (.-message e))))
                 (finally
                   (<! (store/delete-store s opts))
                   (done))))))))

(deftest ^:slow list-stores-test
  (testing "list-stores reflects store creation and deletion"
    (async done
           (let [s1   (spec)
                 s2   (spec)
                 id1  (:id s1)
                 id2  (:id s2)
                 base (dissoc (base-spec) :backend)]
             (go
               (try
                 (let [initial (<! (s3/list-stores base))]
                   (is (not (contains? initial id1)) "id1 absent initially")
                   (is (not (contains? initial id2)) "id2 absent initially"))

                 (let [store1 (<! (store/create-store s1 opts))
                       store2 (<! (store/create-store s2 opts))]
                   (let [stores (<! (s3/list-stores base))]
                     (is (contains? stores id1) "id1 registered after create")
                     (is (contains? stores id2) "id2 registered after create"))

                   ;; Remove one; the other must remain.
                   (<! (store/release-store s1 store1 opts))
                   (<! (store/delete-store s1 opts))
                   (let [stores (<! (s3/list-stores base))]
                     (is (not (contains? stores id1)) "id1 gone after delete")
                     (is (contains? stores id2) "id2 still present"))

                   (<! (store/release-store s2 store2 opts))
                   (<! (store/delete-store s2 opts))
                   (let [stores (<! (s3/list-stores base))]
                     (is (not (contains? stores id1)))
                     (is (not (contains? stores id2)) "both gone after delete")))
                 (catch :default e
                   (is false (str "list-stores-test threw: " (.-message e))))
                 (finally
                   (<! (store/delete-store s1 opts))
                   (<! (store/delete-store s2 opts))
                   (done))))))))

(deftest ^:slow fenced-concurrent-idempotent-operations-test
  (testing "concurrent idempotent operations converge when the caller fences and retries.

            This replaces a test that expected plain concurrent `update-in` to
            converge on its own. It did, under the old design, because an ETag
            left in a process-local cache was applied as an implicit If-Match —
            which is precisely why that design was unsound: a cold cache, a fresh
            connection, or the retries knob at its default turned the guarantee
            off with nothing to notice, and the test could not tell the two apart.
            Fencing is now something the caller asks for, so the retry loop that
            makes it converge belongs to the caller too."
    (async done
           (let [s           (spec)
                 opts        {:sync? false}
                 num-workers 3
                 per-worker  5
                 expected    (set (for [worker-id (range num-workers)
                                        operation-id (range per-worker)]
                                    [worker-id operation-id]))
                 exhausted   (atom [])]
             (go
               (try
                 (let [init (<! (store/create-store s opts))]
                   (<! (k/assoc-in init [:completed] #{} opts))
                   (<! (store/release-store s init opts)))

                 (let [worker (fn [worker-id]
                                (go
                                  (let [ws (<! (store/connect-store s opts))]
                                    (dotimes [operation-id per-worker]
                                      ;; Read the revision, write against it, and
                                      ;; retry from a RE-READ after any error — a
                                      ;; conflict against the same revision would
                                      ;; just be rejected again forever.
                                      ;; A unique token makes an ambiguous network
                                      ;; result safe to retry too: applying `conj`
                                      ;; twice has the same result as applying it
                                      ;; once. A successful stale overwrite still
                                      ;; loses another token and fails the final
                                      ;; exact-set assertion.
                                      (let [token [worker-id operation-id]]
                                        (loop [tries 0]
                                          (let [rev (<! (k/revision ws :completed opts))
                                                res (if (instance? js/Error rev)
                                                      rev
                                                      (<! (k/update-in ws [:completed]
                                                                       #(conj (or % #{}) token)
                                                                       (assoc opts :expected-revision rev))))]
                                            (cond
                                              (not (instance? js/Error res)) :done
                                              (>= tries 200)
                                              (swap! exhausted conj
                                                     [token (:type (ex-data res)) (ex-message res)])
                                              :else (recur (inc tries)))))))
                                    (<! (store/release-store s ws opts))
                                    :done)))
                       chans  (mapv worker (range num-workers))]
                   (loop [[c & more] chans]
                     (when c
                       (<! c)
                       (recur more))))

                 (let [fin   (<! (store/connect-store s opts))
                       final (<! (k/get-in fin [:completed] nil opts))]
                   (is (empty? @exhausted)
                       (str "no operation may give up after 200 retries: "
                            (pr-str @exhausted)))
                   (is (= expected final)
                       (str "missing or unexpected operation tokens: expected "
                            (pr-str expected) ", got " (pr-str final)
                            " — a fenced write that lands must not overwrite one it did not see"))
                   (<! (store/release-store s fin opts)))
                 (catch :default e
                   (is false (str "fenced-concurrent-idempotent-operations-test threw: "
                                  (.-message e))))
                 (finally
                   (<! (store/delete-store s opts))
                   (done))))))))
