(ns konserve-s3.batch-gc-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as a]
            [konserve-s3.core :as s3]
            [konserve-s3.storage :as naming]
            [konserve.impl.storage-layout :as layout]
            [konserve.impl.defaults :as defaults]
            [konserve.core :as k]
            [konserve.utils :as utils])
  (:import [java.lang.reflect Proxy InvocationHandler]
           [software.amazon.awssdk.services.s3 S3Client]
           [software.amazon.awssdk.services.s3.model DeleteObjectsRequest DeleteObjectsResponse
            DeletedObject S3Error ListObjectsV2Request ListObjectsV2Response S3Object]))

(defn- client [f]
  (Proxy/newProxyInstance (.getClassLoader S3Client) (into-array Class [S3Client])
                          (reify InvocationHandler
                            (invoke [_ _ method args] (f (.getName method) (when args (aget args 0)))))))

(defn- response [deleted failed]
  (-> (DeleteObjectsResponse/builder)
      (.deleted ^java.util.Collection (mapv #(-> (DeletedObject/builder) (.key %) (.build)) deleted))
      (.errors ^java.util.Collection (mapv (fn [[key code]] (-> (S3Error/builder) (.key key) (.code code) (.build))) failed))
      (.build)))

(defn- result [x sync?]
  (let [v (if sync? x (a/<!! x))] (if (instance? Throwable v) (throw v) v)))

(deftest deletion-splits-provider-batches-and-retries-only-transient-failures
  (doseq [sync? [true false]]
    (let [keys (mapv (fn [_] (str (random-uuid) ".ksv")) (range 1001))
          denied (naming/->key "tenant" (first keys))
          retry-key (naming/->key "tenant" (second keys))
          calls (atom [])
          attempts (atom 0)
          c (client (fn [method ^DeleteObjectsRequest req]
                      (is (= "deleteObjects" method))
                      (let [objects (mapv #(.key %) (.objects (.delete req)))
                            attempt (swap! attempts inc)
                            errors (if (= attempt 1) {denied "AccessDenied" retry-key "SlowDown"} {})]
                        (swap! calls conj objects)
                        (response (remove #(contains? errors %) objects) errors))))
          etags (atom (zipmap (map #(naming/->key "tenant" %) keys) (repeat :etag)))
          backing (s3/->S3Bucket c "bucket" "tenant" etags)
          r (result (layout/-batch-delete-blobs backing keys {:sync? sync? :delete-retries 1}) sync?)]
      (is (= (disj (set keys) (first keys)) (:deleted r)))
      (is (= {(first keys) {:code "AccessDenied" :message nil}} (:failed r)))
      (is (= [1000 1 1] (mapv count @calls)))
      (is (= [retry-key] (second @calls)))
      (is (empty? @etags)))))

(deftest backend-errors-and-malformed-responses-clear-etags
  (doseq [sync? [true false]]
    (let [key (str (random-uuid) ".ksv")
          object (naming/->key "tenant" key)]
      (doseq [reply [(fn [& _] (throw (ex-info "Transport" {:type :test/transport})))
                     (fn [& _] (response [] {}))]]
        (let [etags (atom {object :stale})
              backing (s3/->S3Bucket (client reply) "bucket" "tenant" etags)
              e (try (result (layout/-batch-delete-blobs backing [key] {:sync? sync?}) sync?)
                     (catch Exception e e))]
          (is (contains? #{:test/transport :konserve.s3/batch-delete-invalid-result} (:type (ex-data e))))
          (is (empty? @etags)))))))

(deftest native-delete-capability-does-not-advertise-atomic-multi-writes
  (let [key (str (random-uuid) ".ksv")
        backing (s3/->S3Bucket (client (fn [_ ^DeleteObjectsRequest req]
                                         (response (map #(.key %) (.objects (.delete req))) {})))
                               "bucket" "tenant" (atom {}))
        store (defaults/map->DefaultStore {:backing backing :config {:in-place? true} :locks (atom {})})]
    (is (k/batch-delete-capable? store))
    (is (= #{:missing} (k/batch-dissoc store [:missing] {:sync? true})))
    (is (not (utils/multi-key-capable? store)))
    (is (empty? @(:locks store)))))

(deftest exhausted-transient-errors-stay-failed
  (doseq [sync? [true false] retries [0 2]]
    (let [key (str (random-uuid) ".ksv")
          object (naming/->key "tenant" key)
          calls (atom 0)
          etags (atom {object :etag})
          backing (s3/->S3Bucket (client (fn [& _]
                                           (swap! calls inc)
                                           (response [] {object "SlowDown"})))
                                 "bucket" "tenant" etags)
          r (result (layout/-batch-delete-blobs backing [key]
                                                {:sync? sync? :delete-retries retries}) sync?)]
      (is (= (inc retries) @calls))
      (is (empty? (:deleted r)))
      (is (= #{key} (set (keys (:failed r)))))
      (is (empty? @etags)))))

(deftest partial-backend-failure-reaches-public-api-and-releases-locks
  (doseq [sync? [true false]]
    (let [denied (naming/->key "tenant" (defaults/key->store-key :denied))
          backing (s3/->S3Bucket (client (fn [_ ^DeleteObjectsRequest req]
                                           (let [requested (map #(.key %) (.objects (.delete req)))]
                                             (response (remove #{denied} requested) {denied "AccessDenied"}))))
                                 "bucket" "tenant" (atom {}))
          store (defaults/map->DefaultStore {:backing backing :config {:in-place? true} :locks (atom {})})
          e (try (result (k/batch-dissoc store [:ok :denied] {:sync? sync?}) sync?)
                 (catch Exception e e))]
      (is (= :konserve/batch-delete-incomplete (:type (ex-data e))))
      (is (= #{:ok} (:deleted (ex-data e))))
      (is (= #{:denied} (set (keys (:failed (ex-data e))))))
      (is (empty? @(:locks store))))))

(defn- page-response [keys cursor]
  (-> (ListObjectsV2Response/builder)
      (.contents ^java.util.Collection (mapv #(-> (S3Object/builder) (.key %) (.build)) keys))
      (.isTruncated (boolean cursor))
      (.nextContinuationToken cursor)
      (.build)))

(deftest physical-pages-preserve-empty-continuations-and-store-isolation
  (doseq [sync? [true false]]
    (let [key (str (random-uuid) ".ksv")
          requests (atom [])
          c (client (fn [method ^ListObjectsV2Request req]
                      (is (= "listObjectsV2" method))
                      (swap! requests conj [(.prefix req) (.continuationToken req) (.maxKeys req)])
                      (if (nil? (.continuationToken req))
                        (page-response [(naming/marker-key "tenant") (str "tenant_nested_" key)] "next")
                        (page-response [(naming/->key "tenant" key)] nil))))
          backing (s3/->S3Bucket c "bucket" "tenant" (atom {}))]
      (is (= {:keys [] :cursor "next"}
             (result (layout/-key-page-blobs backing nil {:sync? sync? :limit 5}) sync?)))
      (is (= {:keys [key] :cursor nil}
             (result (layout/-key-page-blobs backing "next" {:sync? sync? :limit 5}) sync?)))
      (is (= [["tenant_" nil 5] ["tenant_" "next" 5]] @requests)))))

(deftest truncated-pages-without-progress-are-rejected
  (let [c (client (fn [& _]
                    (-> (ListObjectsV2Response/builder) (.isTruncated true) (.build))))]
    (is (= :konserve.s3/list-page-stalled
           (try (s3/list-object-page c "bucket" "tenant_" nil 1000)
                (catch Exception e (:type (ex-data e))))))))
