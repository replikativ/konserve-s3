(ns konserve-s3.browser-compliance-test
  "Run the S3 backend from a real browser (ChromeHeadless via karma)
   against a live S3-compatible endpoint. This is the browser twin of
   the node-only compliance-test / minio-test and the only coverage
   that actually exercises the cross-origin path: signed fetch
   preflight, and reading the `ETag` response header (which requires the bucket
   CORS policy to expose it) for optimistic locking.

   The browser can't read process.env, so config comes from `goog-define`d
   constants, defaulting to the docker-compose MinIO used in CI. Override at
   build time via :closure-defines, e.g.

     {konserve-s3.browser-compliance-test/ENDPOINT \"https://...\"
      konserve-s3.browser-compliance-test/BUCKET   \"my-bucket\" ...}

   Excluded from the network-free browser/karma builds; it has its own shadow
   build (:integration) and karma config (karma.integration.conf.js)."
  (:require [clojure.core.async :refer [go <!] :include-macros true]
            [cljs.test :refer [deftest is testing async]]
            [konserve.compliance-test :refer [async-compliance-test]]
            [konserve.core :as k]
            [konserve.store :as store]
            [konserve-s3.core :as s3]))

(goog-define ENDPOINT   "http://localhost:9000")
(goog-define BUCKET     "konserve-test")
(goog-define ACCESS_KEY "minioadmin")
(goog-define SECRET     "minioadmin")
(goog-define REGION     "us-east-1")
(goog-define PATH_STYLE true)

(defn- spec []
  {:endpoint    ENDPOINT
   :bucket      BUCKET
   :access-key  ACCESS_KEY
   :secret      SECRET
   :region      REGION
   :path-style? PATH_STYLE
   :id          (random-uuid)})

(def ^:private opts {:sync? false})

(deftest ^:slow browser-async-compliance
  (testing "konserve async compliance suite runs against S3 from the browser"
    (async done
           (let [s (spec)]
             (go
               (try
                 (let [store (<! (s3/connect-s3-store s :opts opts))]
                   (is (some? store) "connect-s3-store should yield a store")
                   (<! (async-compliance-test store)))
                 (catch :default e
                   (is false (str "browser compliance run threw: " (.-message e))))
                 (finally
                   (<! (s3/delete-s3-store s :opts opts))
                   (done))))))))

(deftest ^:slow browser-fenced-concurrent-counter
  (testing "concurrent increments converge in a BROWSER when the caller fences and
            retries, which is only possible if the ETag is visible here.

            The value-add over the compliance run is unchanged and now sharper: a
            browser can only fence if CORS exposes the ETag header and MinIO
            honours the conditional PUT. It used to be possible for that to fail
            silently — the old design applied a cached ETag as an implicit
            If-Match and fell back to an unconditional PUT when it had none, so a
            browser that could not see the header just wrote unconditionally and
            the test still converged, for a reason it never asserted. Now a fenced
            write with no ETag is REFUSED, so an invisible header fails loudly."
    (async done
           (let [s           (assoc (spec) :backend :s3)
                 num-workers 3
                 per-worker  5
                 expected    (* num-workers per-worker)
                 conflicts   (atom 0)
                 unexpected  (atom [])]
             (go
               (try
                 (let [init (<! (store/create-store s opts))]
                   (is (= :global (k/conditional-write-domain init))
                       "the ETag must be readable from a browser for this to fence")
                   (<! (k/assoc-in init [:counter] 0 opts))
                   (<! (store/release-store s init opts)))
                 (let [worker (fn []
                                (go
                                  (let [ws (<! (store/connect-store s opts))]
                                    (dotimes [_ per-worker]
                                      ;; Read the revision, write against it, retry
                                      ;; from a RE-READ one on conflict. Every
                                      ;; outcome is accounted for: treating a
                                      ;; non-conflict error as success would skip an
                                      ;; increment and show up only as a short count.
                                      (loop [tries 0]
                                        (let [rev (<! (k/revision ws :counter opts))
                                              res (<! (k/update-in ws [:counter] (fnil inc 0)
                                                                   (assoc opts :expected-revision rev)))
                                              t   (:type (ex-data res))]
                                          (cond
                                            (nil? t) :done
                                            (not= :konserve/revision-mismatch t)
                                            (swap! unexpected conj [t (ex-message res)])
                                            (>= tries 200) (swap! unexpected conj [:retries-exhausted])
                                            :else (do (swap! conflicts inc)
                                                      (recur (inc tries)))))))
                                    (<! (store/release-store s ws opts))
                                    :done)))
                       chans  (mapv (fn [_] (worker)) (range num-workers))]
                   (loop [[c & more] chans]
                     (when c (<! c) (recur more))))
                 (let [fin   (<! (store/connect-store s opts))
                       final (<! (k/get-in fin [:counter] nil opts))]
                   (is (empty? @unexpected)
                       (str "no increment may fail for a reason other than a conflict: "
                            (pr-str @unexpected)))
                   ;; Without this the test can pass vacuously: if the workers
                   ;; never raced, convergence shows the arithmetic worked, not
                   ;; that anything was fenced. The node and JVM twins assert it;
                   ;; this one did not until it was checked.
                   (is (pos? @conflicts)
                       (str "the workers must actually have contended (" @conflicts
                            " conflicts); a run with none proves nothing about the fence"))
                   (is (= expected final)
                       (str "expected " expected " increments but got " final))
                   (<! (store/release-store s fin opts)))
                 (catch :default e
                   (is false (str "browser fenced-counter threw: " (.-message e))))
                 (finally
                   (<! (store/delete-store s opts))
                   (done))))))))
