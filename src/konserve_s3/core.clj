(ns konserve-s3.core
  "S3 based konserve backend."
  (:require [konserve.impl.defaults :refer [connect-default-store]]
            [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock -delete-store header-size]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [konserve.store :as store]
            [superv.async :refer [go-try- <?-]]
            [taoensso.timbre :refer [info trace]]
            [clojure.core.async :refer [chan go]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.util Arrays UUID]
           ;; AWS API
           [software.amazon.awssdk.regions Region]
           [com.amazonaws.xray.interceptors TracingInterceptor]
           [software.amazon.awssdk.core.client.config ClientOverrideConfiguration]
           [software.amazon.awssdk.auth.credentials EnvironmentVariableCredentialsProvider AwsBasicCredentials StaticCredentialsProvider]
           [software.amazon.awssdk.http.urlconnection UrlConnectionHttpClient]
           [software.amazon.awssdk.core ResponseInputStream SdkBytes]
           [software.amazon.awssdk.http AbortableInputStream]
           ;; AWS S3 API
           ;; https://sdk.amazonaws.com/java/api/latest/index.html?software/amazon/awssdk/services/s3/package-summary.html
           [software.amazon.awssdk.services.s3 S3Client S3Configuration]
           [software.amazon.awssdk.services.s3.model S3Object S3Request S3Exception
            CreateBucketRequest CreateBucketResponse
            DeleteBucketRequest DeleteBucketResponse
            HeadBucketRequest HeadBucketResponse
            ListObjectsRequest ListObjectsResponse
            GetObjectRequest GetObjectResponse
            PutObjectRequest PutObjectRequest
            CopyObjectRequest Delete DeleteObjectRequest DeleteObjectsRequest HeadObjectRequest
            NoSuchBucketException NoSuchKeyException
            ObjectIdentifier]
           [software.amazon.awssdk.core.sync RequestBody]))

#_(set! *warn-on-reflection* 1)

(def ^:const default-bucket "konserve")
(def ^:const output-stream-buffer-size (* 1024 1024))
(def ^:const deletion-batch-size 1000)

(def regions (into {} (map (fn [r] [(.toString r) r]) (Region/regions))))

(defn common-client-config
  [client {:keys [region x-ray? access-key secret endpoint-override path-style-access?]}]
  (-> client
      (cond-> region (.region (if (= region "auto") (Region/of region) (regions region)))
              x-ray? (.overrideConfiguration (-> (ClientOverrideConfiguration/builder)
                                                 (.addExecutionInterceptor (TracingInterceptor.))
                                                 (.build)))
              access-key (.credentialsProvider (StaticCredentialsProvider/create (AwsBasicCredentials/create access-key secret)))
              endpoint-override (.endpointOverride (java.net.URI. (str (name (:protocol endpoint-override)) "://"
                                                                       (:hostname endpoint-override)
                                                                       (when-let [port (:port endpoint-override)] (str ":" port))
                                                                       (:path endpoint-override "")))))
      (.httpClientBuilder (UrlConnectionHttpClient/builder))))

(defn s3-client
  [opts]
  (let [builder (-> (S3Client/builder)
                    (common-client-config opts))]
    (when (:path-style-access? opts)
      (.serviceConfiguration builder
                             (reify java.util.function.Consumer
                               (accept [_ s3-config-builder]
                                 (.pathStyleAccessEnabled s3-config-builder true)))))
    (.build builder)))

(defn bucket-exists? [client bucket]
  (try
    (.headBucket client (-> (HeadBucketRequest/builder)
                            (.bucket bucket)
                            (.build)))
    true
    (catch NoSuchBucketException _
      false)))

(defn create-bucket [client bucket]
  (.createBucket client (-> (CreateBucketRequest/builder)
                            (.bucket bucket)
                            (.build))))

(defn delete-bucket [client bucket]
  (.deleteBucket client (-> (DeleteBucketRequest/builder)
                            (.bucket bucket)
                            (.build))))

(defn put-object [^S3Client client ^String bucket ^String key ^bytes bytes]
  (.putObject client
              (-> (PutObjectRequest/builder)
                  (.bucket bucket)
                  (.key key)
                  (.build))
              ^RequestBody (RequestBody/fromBytes bytes)))

(defn get-object [^S3Client client bucket key]
  (try
    (let [res (.getObject client
                          ^S3Request (-> (GetObjectRequest/builder)
                                         (.bucket bucket)
                                         (.key key)
                                         (.build)))
          out (.readAllBytes res)]
      (.close res)
      out)
    (catch NoSuchKeyException _
      nil)))

(defn get-object-with-etag
  "Get object and return map with :data and :etag, or nil if not found."
  [^S3Client client bucket key]
  (try
    (let [response (.getObject client
                               ^S3Request (-> (GetObjectRequest/builder)
                                              (.bucket bucket)
                                              (.key key)
                                              (.build)))
          data (.readAllBytes ^ResponseInputStream response)
          etag (.response ^ResponseInputStream response)]
      (.close response)
      {:data data
       :etag (.eTag etag)})
    (catch NoSuchKeyException _
      nil)))

(defn put-object-conditional
  "Put object with conditional ETag check. Returns true on success, false on conflict.
   S3 returns HTTP 412 (Precondition Failed) when ifMatch ETag doesn't match."
  [^S3Client client ^String bucket ^String key ^bytes bytes if-match-etag]
  (try
    (.putObject client
                (-> (PutObjectRequest/builder)
                    (.bucket bucket)
                    (.key key)
                    (.ifMatch if-match-etag)
                    (.build))
                ^RequestBody (RequestBody/fromBytes bytes))
    true
    (catch S3Exception e
      (if (= 412 (.statusCode e))
        false  ; Precondition failed - ETag mismatch
        (throw e)))))

(defn exists? [^S3Client client bucket key]
  (try
    (.headObject client
                 ^S3Request (-> (HeadObjectRequest/builder)
                                (.bucket bucket)
                                (.key key)
                                (.build)))
    true
    (catch NoSuchKeyException _
      false)))

(defn list-objects
  [^S3Client client bucket]
  (let [request (-> (ListObjectsRequest/builder)
                    (.bucket bucket)
                    (.build))]
    (doall (map #(.key %) (.contents (.listObjects client request))))))

(defn copy [client bucket source-key destination-key]
  (.copyObject client (-> (CopyObjectRequest/builder)
                          (.sourceBucket bucket)
                          (.sourceKey source-key)
                          (.destinationBucket bucket)
                          (.destinationKey destination-key)
                          (.build))))

(defn delete [client bucket key]
  (.deleteObject client (-> (DeleteObjectRequest/builder)
                            (.bucket bucket)
                            (.key key)
                            (.build))))

(defn delete-keys [client bucket keys]
  (let [keys-ids (map (fn [key] (-> (ObjectIdentifier/builder)
                                    (.key key)
                                    (.build)))
                      keys)]
    (.deleteObjects client (-> (DeleteObjectsRequest/builder)
                               (.bucket bucket)
                               (.delete (-> (Delete/builder)
                                            (.objects keys-ids)
                                            (.build)))
                               (.build)))))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defn ->key [store-id key]
  (str store-id "_" key))

(defn- marker-key
  "Returns the S3 key for the store metadata marker file."
  [store-id]
  (str store-id "_.konserve-metadata"))

(def ^:private registry-key "_konserve-stores-registry")

(defn- serialize-registry
  "Serialize a set of UUIDs to bytes."
  [store-ids]
  (.getBytes (pr-str (vec store-ids)) "UTF-8"))

(defn- deserialize-registry
  "Deserialize bytes to a set of UUIDs."
  [^bytes data]
  (if data
    (set (read-string (String. data "UTF-8")))
    #{}))

(defn- update-registry
  "Update the stores registry with optimistic concurrency control using ETags.
   modify-fn takes the current set of UUIDs and returns the new set.
   Retries up to max-retries times on conflicts."
  ([client bucket modify-fn]
   (update-registry client bucket modify-fn 5))
  ([client bucket modify-fn max-retries]
   (loop [attempt 0]
     (let [current (get-object-with-etag client bucket registry-key)
           current-set (deserialize-registry (:data current))
           new-set (modify-fn current-set)
           new-data (serialize-registry new-set)]
       (if current
         ;; Registry exists - conditional update
         (if (put-object-conditional client bucket registry-key new-data (:etag current))
           new-set
           (if (< attempt max-retries)
             (do
               (trace "Registry update conflict, retrying..." attempt)
               (recur (inc attempt)))
             (throw (ex-info "Registry update failed after max retries"
                             {:max-retries max-retries}))))
         ;; Registry doesn't exist - create it
         (do
           (put-object client bucket registry-key new-data)
           new-set))))))

(defrecord S3Blob [bucket key data fetched-object etag]
  PBackingBlob
  (-sync [this env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [{:keys [header meta value]} @data
                       baos (ByteArrayOutputStream. output-stream-buffer-size)
                       ;; Get ETag from bucket's cache (set during read)
                       current-etag (when-let [cache (:etag-cache bucket)]
                                      (get @cache key))
                       optimistic-locking-retries (get-in env [:config :optimistic-locking-retries] 0)]
                   (if (and header meta value)
                     (do
                       (.write baos header)
                       (.write baos meta)
                       (.write baos value)
                       (let [bytes (.toByteArray baos)]
                         (if (and (pos? optimistic-locking-retries) current-etag)
                           ;; Use conditional PUT with ETag - throw on conflict for retry at io-operation level
                           (when-not (put-object-conditional (:client bucket)
                                                             (:bucket bucket)
                                                             key
                                                             bytes
                                                             current-etag)
                             (throw (ex-info "Optimistic lock conflict"
                                             {:type :optimistic-lock-conflict
                                              :key key
                                              :etag current-etag})))
                           ;; Regular PUT without ETag check
                           (put-object (:client bucket)
                                       (:bucket bucket)
                                       key
                                       bytes)))
                       (.close baos))
                     (throw (ex-info "Updating a row is only possible if header, meta and value are set."
                                     {:data @data})))
                   (reset! data {})
                   (reset! etag nil)
                   (reset! fetched-object nil)
                   ;; Clear ETag from cache after successful write
                   (when-let [cache (:etag-cache bucket)]
                     (swap! cache dissoc key))))))
  (-close [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-get-lock [_ env]
    (if (:sync? env) true (go-try- true)))                       ;; May not return nil, otherwise eternal retries
  (-read-header [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 ;; first access is always to header, after it is cached
                 (when-not @fetched-object
                   (let [optimistic-locking-retries (get-in env [:config :optimistic-locking-retries] 0)
                         response (if (pos? optimistic-locking-retries)
                                    ;; Fetch with ETag for optimistic locking
                                    (get-object-with-etag (:client bucket) (:bucket bucket) key)
                                    ;; Regular fetch without ETag
                                    {:data (get-object (:client bucket) (:bucket bucket) key)
                                     :etag nil})]
                     (reset! fetched-object (:data response))
                     ;; Store ETag in bucket's cache for later use
                     (when (:etag response)
                       (reset! etag (:etag response))
                       (when-let [cache (:etag-cache bucket)]
                         (swap! cache assoc key (:etag response))))))
                 (Arrays/copyOfRange ^bytes @fetched-object (int 0) (int header-size)))))
  (-read-meta [_ meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (Arrays/copyOfRange ^bytes @fetched-object (int header-size) (int (+ header-size meta-size))))))
  (-read-value [_ meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [obj ^bytes @fetched-object]
                   (Arrays/copyOfRange obj (int (+ header-size meta-size)) (int (alength obj)))))))
  (-read-binary [_ meta-size locked-cb env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [obj ^bytes @fetched-object]
                   (locked-cb {:input-stream
                               (ByteArrayInputStream.
                                (Arrays/copyOfRange obj (int (+ header-size meta-size)) (int (alength obj))))
                               :size (- (alength obj) (+ header-size meta-size))})))))

  (-write-header [_ header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :header header))))
  (-write-meta [_ meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :meta meta))))
  (-write-value [_ value _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value))))
  (-write-binary [_ _meta-size blob env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value blob)))))

(defrecord S3Bucket [client bucket store-id etag-cache]
  PBackingStore
  (-create-blob [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (S3Blob. this (->key store-id store-key) (atom {}) (atom nil) (atom nil)))))
  (-delete-blob [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (delete client bucket (->key store-id store-key)))))
  (-blob-exists? [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (exists? client bucket (->key store-id store-key)))))
  (-copy [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (copy client bucket (->key store-id from) (->key store-id to)))))
  (-atomic-move [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (copy client bucket (->key store-id from) (->key store-id to))
                 (delete client bucket (->key store-id from)))))
  (-migratable [_ _key _store-key env]
    (if (:sync? env) nil (go-try- nil)))
  (-migrate [_ _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-create-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when-not (bucket-exists? client bucket)
                   (create-bucket client bucket))
                 ;; Write marker file to indicate store exists
                 (put-object client bucket (marker-key store-id) (.getBytes "konserve"))
                 ;; Add store-id to registry with optimistic concurrency control
                 (let [store-uuid (UUID/fromString store-id)]
                   (update-registry client bucket #(conj % store-uuid))))))
  (-store-exists? [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (exists? client bucket (marker-key store-id)))))
  (-sync-store [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (when (bucket-exists? client bucket)
                           (info "This will delete all konserve files, but won't delete the bucket. You can use konserve-s3.core/delete-bucket if you intend to delete the bucket as well.")
                           (doseq [keys (->> (list-objects client bucket)
                                             (filter (fn [^String key]
                                                       (and (.startsWith key store-id)
                                                            (or (.endsWith key ".ksv")
                                                                (.endsWith key ".ksv.new")
                                                                (.endsWith key ".ksv.backup")
                                                                (.endsWith key ".konserve-metadata")))))
                                             (partition deletion-batch-size deletion-batch-size []))]
                             (trace "deleting keys: " keys)
                             (delete-keys client bucket keys))
                           ;; Remove store-id from registry with optimistic concurrency control
                           (let [store-uuid (UUID/fromString store-id)]
                             (update-registry client bucket #(disj % store-uuid)))
                           (.close client)))))
  (-keys [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [keys (list-objects client bucket)]
                   (->> (filter (fn [^String key]
                                  (and (.startsWith key store-id)
                                       (or (.endsWith key ".ksv")
                                           (.endsWith key ".ksv.new")
                                           (.endsWith key ".ksv.backup"))))
                                keys)
                          ;; remove store-id prefix
                        (map #(subs % (inc (count store-id))))))))))

(defn connect-store [s3-spec & {:keys [opts]
                                :as params}]
  (let [complete-opts (merge {:sync? true} opts)
        store-id (str (:id s3-spec))
        backing (S3Bucket. (s3-client s3-spec) (:bucket s3-spec) store-id (atom {}))
        ;; Merge user config with defaults
        user-config (:config s3-spec)
        default-config {:sync-blob? true
                        :in-place? true
                        :no-backup? true
                        :lock-blob? true}
        merged-config (merge default-config user-config)
        ;; Only pass non-S3-specific config to connect-default-store
        config {:opts               complete-opts
                :config             merged-config
                :default-serializer :FressianSerializer
                :buffer-size        (* 1024 1024)}]
    (connect-default-store backing config)))

(defn release
  "Must be called after work on database has finished in order to close connection"
  [store env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try- (.close ^S3Client (:client (:backing store))))))

(defn delete-store [s3-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        store-id (str (:id s3-spec))
        backing (S3Bucket. (s3-client s3-spec) (:bucket s3-spec) store-id (atom {}))]
    (-delete-store backing complete-opts)))

(defn list-stores
  "List all konserve stores in an S3 bucket by reading the central registry.

   The registry uses optimistic concurrency control (ETags) to handle concurrent
   updates from multiple processes/machines.

   Args:
     s3-spec - Map with :bucket, :region, :access-key, :secret, etc.
     opts - (optional) Runtime options map. Defaults to {:sync? true}

   Returns:
     Set of UUIDs representing store IDs in the bucket"
  [s3-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        client (s3-client s3-spec)
        bucket (:bucket s3-spec)]
    (async+sync (:sync? complete-opts) *default-sync-translation*
                (go-try-
                 (try
                   (let [data (get-object client bucket registry-key)]
                     (deserialize-registry data))
                   (finally
                     (.close client)))))))

(comment

  (require '[konserve.core :as k])

  (def s3-spec {:region   "us-west-1"
                :bucket   "konserve-s3"
                :store-id "test2"
                :x-ray?   true
                :access-key "ACCESS_KEY"
                :password "SECRET"})

  (def s3-spec-alt {:region   "auto"
                    :bucket   "konserve-s3"
                    :store-id "test2"
                    :endpoint-override {:protocol :https
                                        :hostname "Minio or other S3 compatible store"
                                        :port "Optional port number"}
                    :access-key "ACCESS_KEY"
                    :secret "SECRET"})

  (def test-client (s3-client s3-spec))

  (delete-store s3-spec :opts {:sync? true})

  (def store (connect-store s3-spec :opts {:sync? true}))

  (time (k/assoc-in store ["foo"] {:foo "baz"} {:sync? true}))

  (k/get-in store ["foo"] nil {:sync? true})

  (k/exists? store "foo" {:sync? true})

  (time (k/assoc-in store [:bar] 42 {:sync? true}))

  (k/update-in store [:bar] inc {:sync? true})

  (k/get-in store [:bar] nil {:sync? true})

  (k/dissoc store :bar {:sync? true})

  (k/append store :error-log {:type :horrible} {:sync? true})

  (k/log store :error-log {:sync? true})

  (k/keys store {:sync? true})

  (k/bassoc store :binbar (byte-array (range 10)) {:sync? true})

  (k/bget store :binbar (fn [{:keys [input-stream]}]
                          (map byte (slurp input-stream)))
          {:sync? true})

  (release store {:sync? true}))

(comment

  (require '[konserve.core :as k])
  (require '[clojure.core.async :refer [<!!]])

  (<!! (delete-store s3-spec :opts {:sync? false}))

  (def store (<!! (connect-store s3-spec :opts {:sync? false})))

  (time (<!! (k/assoc-in store ["foo" :bar] {:foo "baz"} {:sync? false})))

  (<!! (k/get-in store ["foo"] nil {:sync? false}))

  (<!! (k/exists? store "foo" {:sync? false}))

  (time (<!! (k/assoc-in store [:bar] 42 {:sync? false})))

  (<!! (k/update-in store [:bar] inc {:sync? false}))
  (<!! (k/get-in store [:bar] nil {:sync? false}))
  (<!! (k/dissoc store :bar {:sync? false}))

  (<!! (k/append store :error-log {:type :horrible} {:sync? false}))
  (<!! (k/log store :error-log {:sync? false}))

  (<!! (k/keys store {:sync? false}))

  (<!! (k/bassoc store :binbar (byte-array (range 10)) {:sync? false}))
  (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                               (map byte (slurp input-stream)))
               {:sync? false}))
  (<!! (release store {:sync? false})))

;; =============================================================================
;; Multimethod Registration for konserve.store dispatch
;; =============================================================================

(defmethod store/-connect-store :s3
  [{:keys [region bucket id] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [s3-spec (dissoc config :backend)
                     store-id (str id)
                     backing (S3Bucket. (s3-client s3-spec) bucket store-id (atom {}))
                     exists (<?- (konserve.impl.storage-layout/-store-exists? backing opts))]
                 (when-not exists
                   (throw (ex-info (str "S3 store does not exist: " bucket "/" store-id)
                                   {:bucket bucket :store-id store-id :region region :config config})))
                 (let [store (<?- (connect-store s3-spec :opts opts))]
                   (assoc store :id id))))))

(defmethod store/-create-store :s3
  [{:keys [region bucket id] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [s3-spec (dissoc config :backend)
                     store-id (str id)
                     backing (S3Bucket. (s3-client s3-spec) bucket store-id (atom {}))
                     exists (<?- (konserve.impl.storage-layout/-store-exists? backing opts))]
                 (when exists
                   (throw (ex-info (str "S3 store already exists: " bucket "/" store-id)
                                   {:bucket bucket :store-id store-id :region region :config config})))
                 (let [store (<?- (connect-store s3-spec :opts opts))]
                   (assoc store :id id))))))

(defmethod store/-store-exists? :s3
  [{:keys [region bucket id] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [s3-spec (dissoc config :backend)
                     store-id (str id)
                     backing (S3Bucket. (s3-client s3-spec) bucket store-id (atom {}))]
                 (<?- (konserve.impl.storage-layout/-store-exists? backing opts))))))

(defmethod store/-delete-store :s3
  [{:keys [region bucket id] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [s3-spec (dissoc config :backend)]
                 (delete-store s3-spec :opts opts)))))

(defmethod store/-release-store :s3
  [_config store opts]
  (release store opts))
