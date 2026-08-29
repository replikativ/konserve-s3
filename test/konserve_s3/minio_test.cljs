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
