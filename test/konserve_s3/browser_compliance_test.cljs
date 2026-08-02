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

(deftest ^:slow browser-optimistic-locking
  (testing "cross-origin ETag optimistic locking: concurrent update-in converges"
    ;; The value-add over compliance: this only passes if the browser can read
    ;; the ETag header (CORS expose) and MinIO honours the conditional PUT.
    (async done
           (let [s           (assoc (spec) :backend :s3
                                    :config {:optimistic-locking-retries 100})
                 num-workers 3
                 per-worker  5
                 expected    (* num-workers per-worker)]
             (go
               (try
                 (let [init (<! (store/create-store s opts))]
                   (<! (k/assoc-in init [:counter] 0 opts))
                   (<! (store/release-store s init opts)))
                 (let [worker (fn []
                                (go
                                  (let [ws (<! (store/connect-store s opts))]
                                    (loop [i 0]
                                      (when (< i per-worker)
                                        (<! (k/update-in ws [:counter] (fnil inc 0) opts))
                                        (recur (inc i))))
                                    (<! (store/release-store s ws opts))
                                    :done)))
                       chans  (mapv (fn [_] (worker)) (range num-workers))]
                   (loop [[c & more] chans]
                     (when c (<! c) (recur more))))
                 (let [fin   (<! (store/connect-store s opts))
                       final (<! (k/get-in fin [:counter] nil opts))]
                   (is (= expected final)
                       (str "expected " expected " increments but got " final))
                   (<! (store/release-store s fin opts)))
                 (catch :default e
                   (is false (str "browser optimistic-locking threw: " (.-message e))))
                 (finally
                   (<! (store/delete-store s opts))
                   (done))))))))
