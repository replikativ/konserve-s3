(ns konserve-s3.core
  "S3 based konserve backend."
  (:require [konserve.impl.defaults :refer [connect-default-store]]
            [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock -delete-store header-size]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [superv.async :refer [go-try-]]
            [taoensso.timbre :refer [info trace]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.util Arrays]
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
           [software.amazon.awssdk.services.s3 S3Client]
           [software.amazon.awssdk.services.s3.model S3Object S3Request
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
  [client {:keys [region x-ray? access-key secret endpoint-override]}] 
  (-> client
      (cond-> region (.region (regions (if (= region "auto") "us-east-1" region)))
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
  (-> (S3Client/builder)
      (common-client-config opts)
      (.build)))

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

(defrecord S3Blob [bucket key data fetched-object]
  PBackingBlob
  (-sync [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [{:keys [header meta value]} @data
                               baos (ByteArrayOutputStream. output-stream-buffer-size)]
                           (if (and header meta value)
                             (do
                               (.write baos header)
                               (.write baos meta)
                               (.write baos value)
                               (put-object (:client bucket)
                                           (:bucket bucket)
                                           key
                                           (.toByteArray baos))
                               (.close baos))
                             (throw (ex-info "Updating a row is only possible if header, meta and value are set."
                                             {:data @data})))
                           (reset! data {})))))
  (-close [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-get-lock [_ env]
    (if (:sync? env) true (go-try- true)))                       ;; May not return nil, otherwise eternal retries
  (-read-header [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                    ;; first access is always to header, after it is cached
                 (when-not @fetched-object
                   (reset! fetched-object (get-object (:client bucket) (:bucket bucket) key)))
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

(defrecord S3Bucket [client bucket store-id]
  PBackingStore
  (-create-blob [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (S3Blob. this (->key store-id store-key) (atom {}) (atom nil)))))
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
                   (create-bucket client bucket)))))
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
                                                                (.endsWith key ".ksv.backup")))))
                                             (partition deletion-batch-size deletion-batch-size []))]
                             (trace "deleting keys: " keys)
                             (delete-keys client bucket keys))
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
        backing (S3Bucket. (s3-client s3-spec) (:bucket s3-spec) (:store-id s3-spec))
        config (merge {:opts               complete-opts
                       :config             {:sync-blob? true
                                            :in-place? false
                                            :lock-blob? true}
                       :default-serializer :FressianSerializer
                       :buffer-size        (* 1024 1024)}
                      (dissoc params :opts :config))]
    (connect-default-store backing config)))

(defn release
  "Must be called after work on database has finished in order to close connection"
  [store env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try- (.close ^S3Client (:client (:backing store))))))

(defn delete-store [s3-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        backing (S3Bucket. (s3-client s3-spec) (:bucket s3-spec) (:store-id s3-spec))]
    (-delete-store backing complete-opts)))

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
