(ns konserve-s3.compliance-test
  "Run konserve's shared async compliance suite against a real
   S3-compatible endpoint (MinIO locally, or Amazon S3 / Cloudflare R2 in CI).

   Network test — it needs a reachable bucket and credentials, supplied via env
   vars. When S3_ENDPOINT is unset it falls back to the docker-compose MinIO at
   localhost:9000. Each run uses a fresh random store id and deletes the store in
   a finally, so aborted/parallel runs cannot collide.

       S3_ENDPOINT    e.g. http://localhost:9000 (MinIO) or
                      https://<account>.r2.cloudflarestorage.com (R2)
       S3_BUCKET      bucket name (must already exist)
       S3_ACCESS_KEY  / S3_SECRET   credentials
       S3_REGION      e.g. us-east-1 / auto
       S3_PATH_STYLE  \"false\" for Amazon virtual-hosted addressing (default true)"
  (:require [clojure.core.async :refer [go <!] :include-macros true]
            [cljs.test :refer [deftest is async]]
            [konserve.compliance-test :refer [async-compliance-test
                                              async-conditional-write-compliance-test]]
            [konserve.core :as k]
            [konserve-s3.core :as s3]))

(defn- env [k]
  (some-> (.-env js/process) (aget k)))

(defn- spec []
  {:endpoint    (or (env "S3_ENDPOINT") "http://localhost:9000")
   :bucket      (or (env "S3_BUCKET") "konserve-test")
   :access-key  (or (env "S3_ACCESS_KEY") "minioadmin")
   :secret      (or (env "S3_SECRET") "minioadmin")
   :region      (or (env "S3_REGION") "us-east-1")
   :path-style? (not= "false" (env "S3_PATH_STYLE"))
   :id          (random-uuid)})

(deftest ^:slow s3-async-compliance
  (async done
         (let [s (spec)]
           (go
             (try
               (let [store (<! (s3/connect-s3-store s :opts {:sync? false}))]
                 (is (some? store) "connect-s3-store should yield a store")
                 (<! (async-compliance-test store)))
               (catch :default e
                 (is false (str "compliance run threw: " (.-message e))))
               (finally
                 (<! (s3/delete-s3-store s :opts {:sync? false}))
                 (done)))))))

(deftest ^:slow s3-conditional-write
  ;; The cljs backend answers `:global`, which is a claim about S3 evaluating the
  ;; precondition rather than about any lock konserve holds — `-get-lock` here is
  ;; a no-op. An unenforced claim of that kind is exactly what the capability
  ;; exists to prevent, so it is exercised against a real endpoint.
  ;;
  ;; The CONTRACT comes from konserve, not from here: restating it in each backend
  ;; is how backends drift apart on the details. konserve's sync suite cannot
  ;; reach this backing (its cljs arm is synchronous, since ClojureScript has no
  ;; blocking take, and this backing is async-only), which is why the async
  ;; variant exists there. Only the S3-specific claim is asserted locally.
  (async done
         (let [s (spec)]
           (go
             (try
               (let [store (<! (s3/connect-s3-store s :opts {:sync? false}))]
                 (is (= :global (k/conditional-write-domain store))
                     "S3 evaluates the precondition, so the domain reaches every writer")
                 (<! (async-conditional-write-compliance-test store)))
               (catch :default e
                 (is false (str "conditional-write run threw: " (.-message e))))
               (finally
                 (<! (s3/delete-s3-store s :opts {:sync? false}))
                 (done)))))))
