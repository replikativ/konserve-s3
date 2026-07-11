(ns konserve-s3.core
  "S3 based konserve backend."
  (:require [konserve.impl.defaults :refer [connect-default-store]]
            [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock PReadMissSafe
                                                  store-key-not-found-ex -delete-store header-size]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [konserve.store :as store]
            [superv.async :refer [go-try- <?-]]
            [replikativ.logging :as log]
            [clojure.core.async :refer [chan go promise-chan put! close!]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.util Arrays UUID]
           [java.util.concurrent Executors ExecutorService ThreadFactory]
           ;; AWS API
           [software.amazon.awssdk.regions Region]
           [com.amazonaws.xray.interceptors TracingInterceptor]
           [software.amazon.awssdk.core.client.config ClientOverrideConfiguration]
           [software.amazon.awssdk.core.interceptor ExecutionInterceptor]
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
            ListObjectsV2Request ListObjectsV2Response
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

(def strip-expect-continue-interceptor
  "The SDK forces `Expect: 100-continue` onto PutObject via an internal
   interceptor regardless of HTTP-client configuration (aws/aws-sdk-java-v2
   #6537). The handshake costs a full extra round trip per PUT — and stalls
   against providers that never send the interim 100 response (Cloudflare R2).
   It only pays off for uploads large enough that resending a rejected body
   hurts, which konserve values are not. Override-config interceptors run
   after the SDK's own, so removing the header here wins."
  (reify ExecutionInterceptor
    (modifyHttpRequest [_ ctx _attrs]
      (let [req (.httpRequest ctx)]
        (if (.isPresent (.firstMatchingHeader req "Expect"))
          (-> req (.toBuilder) (.removeHeader "Expect") (.build))
          req)))))

(defn common-client-config
  [client {:keys [region x-ray? access-key secret endpoint-override path-style-access? expect-continue?]}]
  (-> client
      (.overrideConfiguration (-> (ClientOverrideConfiguration/builder)
                                  (cond-> x-ray? (.addExecutionInterceptor (TracingInterceptor.))
                                          (not expect-continue?) (.addExecutionInterceptor strip-expect-continue-interceptor))
                                  (.build)))
      (cond-> region (.region (if (= region "auto") (Region/of region) (regions region)))
              access-key (.credentialsProvider (StaticCredentialsProvider/create (AwsBasicCredentials/create access-key secret)))
              endpoint-override (.endpointOverride (java.net.URI. (str (name (:protocol endpoint-override)) "://"
                                                                       (:hostname endpoint-override)
                                                                       (when-let [port (:port endpoint-override)] (str ":" port))
                                                                       (:path endpoint-override "")))))
      (.httpClientBuilder (UrlConnectionHttpClient/builder))))

(defn build-s3-client
  "Construct a fresh AWS S3Client from a connection config. Prefer `s3-client`,
   which shares clients across stores."
  [opts]
  (let [builder (-> (S3Client/builder)
                    (common-client-config opts))]
    (when (:path-style-access? opts)
      (.serviceConfiguration builder
                             (reify java.util.function.Consumer
                               (accept [_ s3-config-builder]
                                 (.pathStyleAccessEnabled s3-config-builder true)))))
    (.build builder)))

(def ^:private client-cache
  "client-config -> Delay<S3Client>. The AWS S3Client is thread-safe and
   internally pools HTTP connections; the SDK recommends sharing one per
   endpoint+credentials rather than constructing one per store operation.
   konserve-s3 used to build a fresh client for every -store-exists? /
   -create-store / -connect-store call; they are now shared. Close them with
   `shutdown-clients!` on process exit."
  (java.util.concurrent.ConcurrentHashMap.))

(defn- client-key
  "The subset of a connection config that distinguishes one S3Client from
   another. Bucket and store id are NOT included — a client is endpoint-scoped,
   not store-scoped."
  [opts]
  (select-keys opts [:region :x-ray? :access-key :secret
                     :endpoint-override :path-style-access? :expect-continue?]))

(defn s3-client
  "Return a shared S3Client for the given connection config, constructing one
   on first use and caching it per endpoint+credentials. Safe to call from many
   threads and many stores — they all share one client per endpoint. Call
   `shutdown-clients!` to close the cached clients (e.g. on process exit)."
  [opts]
  @(.computeIfAbsent ^java.util.concurrent.ConcurrentHashMap client-cache
                     (client-key opts)
                     (reify java.util.function.Function
                       (apply [_ _] (delay (build-s3-client opts))))))

(defn shutdown-clients!
  "Close every cached S3Client and clear the cache. Call once on process
   shutdown. After this, `s3-client` constructs fresh clients again."
  []
  (doseq [d (vec (.values ^java.util.concurrent.ConcurrentHashMap client-cache))]
    (when (realized? d)
      (try (.close ^S3Client @d) (catch Throwable _))))
  (.clear ^java.util.concurrent.ConcurrentHashMap client-cache))

;; -----------------------------------------------------------------------------
;; IO instrumentation (off by default; negligible overhead when *io-stats* is nil)
;;
;; Wrap a body of work in `with-io-stats` to count and time the underlying S3
;; operations it performs (put / conditional-put / get / head / list / delete).
;; Used to measure datahike's storage amplification — PUTs-per-commit, GET-per-
;; commit, and per-op latency — independent of any one backend's tuning.
;;
;; NOTE: this lives in the backend as a pragmatic one-off, but its natural home
;; is konserve core: every backend funnels its physical ops through the
;; `PBackingStore` protocol (-create-blob / -read-blob / -delete-blob / -copy /
;; -blob-exists?), so a generic counting/timing decorator there would give ALL
;; backends (s3, gcs, jdbc, file, ...) uniform instrumentation from one place —
;; rather than each backend re-implementing this. If op-counting graduates to a
;; first-class capability, promote it into konserve and drop this section.

(def ^:dynamic *io-stats*
  "When bound to an atom, S3 ops on THIS thread record into it. nil => off.
   Note: datahike's :self writer commits on its own thread, so a dynamic
   binding won't capture those PUTs — use the global accumulator below to
   measure across threads."
  nil)

(defonce ^{:doc "Holds nil or an accumulator atom. When set, S3 ops on ANY
   thread record into it. Use for measuring work that crosses threads (e.g. a
   datahike commit dispatched to the writer actor)."}
  global-io-stats
  (atom nil))

(defn set-global-io-stats!
  "Install acc (an atom) as the cross-thread IO accumulator, or nil to disable."
  [acc]
  (reset! global-io-stats acc))

(defn- record-io! [op ^long nanos]
  (when-let [a (or *io-stats* @global-io-stats)]
    (swap! a (fn [m]
               (-> m
                   (update-in [op :n]  (fnil inc 0))
                   (update-in [op :ns] (fnil + 0) nanos)
                   (update-in [op :samples] (fnil conj []) nanos))))))

(defmacro ^:private timed-io [op & body]
  `(let [t0# (System/nanoTime)
         r#  (do ~@body)]
     (record-io! ~op (- (System/nanoTime) t0#))
     r#))

(defn io-stats-summary
  "Reduce a raw *io-stats* atom value to {op {:n :total-ms :p50-ms :p99-ms}}."
  [m]
  (into {}
        (for [[op {:keys [n ns samples]}] m
              :let [sorted (vec (sort (or samples [])))
                    cnt    (count sorted)
                    pick   (fn [p] (when (pos? cnt)
                                     (/ (nth sorted (min (dec cnt)
                                                         (long (* p cnt))))
                                        1e6)))]]
          [op {:n n
               :total-ms (/ (double (or ns 0)) 1e6)
               :p50-ms (pick 0.50)
               :p99-ms (pick 0.99)}])))

(defmacro with-io-stats
  "Evaluate body with a fresh *io-stats* atom bound on THIS thread. Returns
   {:result <body-value> :stats <io-stats-summary> :raw <atom-value>}.
   Same-thread only — see with-global-io-stats for cross-thread work."
  [& body]
  `(let [a# (atom {})]
     (binding [*io-stats* a#]
       (let [r# (do ~@body)]
         {:result r# :stats (io-stats-summary @a#) :raw @a#}))))

(defmacro with-global-io-stats
  "Evaluate body with a fresh cross-thread accumulator installed, capturing S3
   ops performed on any thread (e.g. datahike's writer actor). NOT reentrant /
   not concurrency-safe — use for isolated single-writer probes. Returns
   {:result :stats :raw} like with-io-stats."
  [& body]
  `(let [a# (atom {})]
     (set-global-io-stats! a#)
     (try
       (let [r# (do ~@body)]
         {:result r# :stats (io-stats-summary @a#) :raw @a#})
       (finally (set-global-io-stats! nil)))))

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
  (timed-io :put
            (.putObject client
                        (-> (PutObjectRequest/builder)
                            (.bucket bucket)
                            (.key key)
                            (.build))
                        ^RequestBody (RequestBody/fromBytes bytes))))

(defn- not-found?
  "True when e (or any exception in its cause chain) signals an S3 'no such
   key' / 404. Walks the cause chain because the SDK calls resolve reflectively
   (S3Client is an interface; Clojure can't pick the overload for these arg
   types), and a reflective invocation wraps the thrown NoSuchKeyException in a
   java.lang.reflect.InvocationTargetException — which a bare
   `(catch NoSuchKeyException ...)` misses on the virtual-thread IO path. Also
   accepts a 404 S3Exception, which some S3-compatible servers return for HEAD
   (no error body to derive the specific NoSuchKey code from)."
  [^Throwable e]
  (boolean
   (some (fn [^Throwable c]
           (or (instance? NoSuchKeyException c)
               (and (instance? S3Exception c) (= 404 (.statusCode ^S3Exception c)))))
         (take-while some? (iterate (fn [^Throwable t] (.getCause t)) e)))))

(defn get-object [^S3Client client bucket key]
  (timed-io :get
            (try
              (let [res (.getObject client
                                    ^GetObjectRequest (-> (GetObjectRequest/builder)
                                                          (.bucket bucket)
                                                          (.key key)
                                                          (.build)))
                    out (.readAllBytes res)]
                (.close res)
                out)
              (catch Exception e
                (if (not-found? e) nil (throw e))))))

(defn get-object-with-etag
  "Get object and return map with :data and :etag, or nil if not found."
  [^S3Client client bucket key]
  (timed-io :get-etag
            (try
              (let [response (.getObject client
                                         ^GetObjectRequest (-> (GetObjectRequest/builder)
                                                               (.bucket bucket)
                                                               (.key key)
                                                               (.build)))
                    data (.readAllBytes ^ResponseInputStream response)
                    etag (.response ^ResponseInputStream response)]
                (.close response)
                {:data data
                 :etag (.eTag etag)})
              (catch Exception e
                (if (not-found? e) nil (throw e))))))

(defn put-object-conditional
  "Put object with conditional ETag check. Returns true on success, false on conflict.
   S3 returns HTTP 412 (Precondition Failed) when ifMatch ETag doesn't match."
  [^S3Client client ^String bucket ^String key ^bytes bytes if-match-etag]
  (timed-io :put-cond
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
                  (throw e))))))

(defn exists? [^S3Client client bucket key]
  (timed-io :head
            (try
              (.headObject client
                           ^HeadObjectRequest (-> (HeadObjectRequest/builder)
                                                  (.bucket bucket)
                                                  (.key key)
                                                  (.build)))
              true
              (catch Exception e
                (if (not-found? e) false (throw e))))))

(defn list-objects
  "List ALL object keys in the bucket, following V2 continuation tokens.
   (The previous implementation issued a single ListObjects call and silently
   returned only the first 1000 keys.)"
  [^S3Client client bucket]
  (loop [continuation nil
         acc          []]
    (let [req (cond-> (ListObjectsV2Request/builder)
                true         (.bucket bucket)
                continuation (.continuationToken continuation))
          ^ListObjectsV2Response rsp (.listObjectsV2 client (.build req))
          acc' (into acc (map #(.key %)) (.contents rsp))]
      (if (.isTruncated rsp)
        (recur (.nextContinuationToken rsp) acc')
        acc'))))

(defn copy [client bucket source-key destination-key]
  (.copyObject client (-> (CopyObjectRequest/builder)
                          (.sourceBucket bucket)
                          (.sourceKey source-key)
                          (.destinationBucket bucket)
                          (.destinationKey destination-key)
                          (.build))))

(defn delete [client bucket key]
  (timed-io :delete
            (.deleteObject client (-> (DeleteObjectRequest/builder)
                                      (.bucket bucket)
                                      (.key key)
                                      (.build)))))

(defn delete-keys [client bucket keys]
  (timed-io :delete-batch
            (let [keys-ids (map (fn [key] (-> (ObjectIdentifier/builder)
                                              (.key key)
                                              (.build)))
                                keys)]
              (.deleteObjects client (-> (DeleteObjectsRequest/builder)
                                         (.bucket bucket)
                                         (.delete (-> (Delete/builder)
                                                      (.objects keys-ids)
                                                      (.build)))
                                         (.build))))))

;; -----------------------------------------------------------------------------
;; Blocking IO offloading
;;
;; The AWS SDK client used here (UrlConnectionHttpClient) is synchronous: every
;; S3 call blocks its thread for a full network round trip. The async branches
;; of the protocol implementations used to run those calls inside `go-try-`,
;; i.e. on core.async's global dispatch pool, which has only ~8 threads. That
;; serialized more than 8 concurrent async ops into waves and starved every
;; other go block in the JVM while S3 calls were in flight. Instead, `io-try-`
;; runs the whole operation body on a virtual thread (JDK 21+) and returns a
;; promise-chan the go/async caller parks on. The sync path is untouched.

(def ^:private vthreads-available?
  "True when java.lang.Thread/startVirtualThread exists (JDK 21+)."
  (boolean
   (try (.getMethod Thread "startVirtualThread" (into-array Class [Runnable]))
        (catch Throwable _ false))))

(defonce ^:private fallback-io-executor
  ;; Only used on JDKs without virtual threads: a fixed pool of daemon threads
  ;; for blocking S3 calls. Caps concurrent S3 IO at 64 but keeps the
  ;; core.async dispatch pool free.
  (delay
   (let [counter (java.util.concurrent.atomic.AtomicInteger.)]
     (Executors/newFixedThreadPool
      64
      (reify ThreadFactory
        (newThread [_ r]
          (doto (Thread. ^Runnable r (str "konserve-s3-io-" (.incrementAndGet counter)))
            (.setDaemon true))))))))

(defn- run-io-task
  "Run r on a fresh virtual thread (JDK 21+), or on the fallback
   platform-thread pool on older JDKs."
  [^Runnable r]
  (if vthreads-available?
    (Thread/startVirtualThread r)
    (.execute ^ExecutorService @fallback-io-executor r)))

(defn- io-thread-ch
  "Run thunk f (blocking S3 IO) off the core.async dispatch pool and return a
   promise-chan that receives its result. Matches `go-try-`'s error-passing
   convention: exceptions are put on the channel so `<?-` rethrows them at the
   take site; non-Exception Throwables are wrapped in an ExceptionInfo so
   `<?-` still detects them. A nil result closes the channel, which is what a
   go block returning nil does."
  [f]
  (let [p (promise-chan)]
    (run-io-task
     (fn []
       (let [res (try (f)
                      (catch Exception e e)
                      (catch Throwable t
                        (ex-info "Error in S3 IO thread." {:type :s3-io-error} t)))]
         (if (nil? res)
           (close! p)
           (put! p res)))))
    p))

(defmacro ^:private io-try-
  "Drop-in replacement for `superv.async/go-try-` for op bodies that perform
   blocking S3 SDK calls: runs the whole body (serialization included) on a
   virtual thread instead of the core.async dispatch pool and returns a
   promise-chan carrying the result or the exception. Rewritten to plain `try`
   in sync mode via `io-sync-translation`."
  [& body]
  `(io-thread-ch (fn [] ~@body)))

(def ^:private io-sync-translation
  "*default-sync-translation* extended so `io-try-` collapses to `try` on the
   sync path, exactly as `go-try-` does."
  (merge *default-sync-translation* '{io-try- try}))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defn ->key [store-id key]
  (str store-id "_" key))

(def ^:private marker-suffix "_.konserve-metadata")

(defn- marker-key
  "Returns the S3 key for the store metadata marker file. Every store has
   exactly one marker object; the set of marker objects IS the store registry
   (see list-stores) — there is no separate central registry object to
   contend on."
  [store-id]
  (str store-id marker-suffix))

;; No central stores-registry: a store's existence is recorded solely by its
;; per-store marker object (see marker-key / -create-store). This removes the
;; single shared object that every -create-store/-delete-store used to CAS,
;; which serialized concurrent store creation. list-stores now derives the
;; set of stores by scanning marker objects.

(defrecord S3Blob [bucket key data fetched-object etag]
  PBackingBlob
  (-sync [this env]
    (async+sync (:sync? env) io-sync-translation
                (io-try-
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
    (async+sync (:sync? env) io-sync-translation
                (io-try-
                 ;; first access is always to header, after it is cached
                 (when-not @fetched-object
                   (let [optimistic-locking-retries (get-in env [:config :optimistic-locking-retries] 0)
                         response (if (pos? optimistic-locking-retries)
                                    ;; Fetch with ETag for optimistic locking
                                    (get-object-with-etag (:client bucket) (:bucket bucket) key)
                                    ;; Regular fetch without ETag
                                    {:data (get-object (:client bucket) (:bucket bucket) key)
                                     :etag nil})]
                     ;; Absent object: get-object returned nil. Signal not-found
                     ;; so io-operation's read-first path (PReadMissSafe below)
                     ;; returns the caller's not-found instead of NPE-ing on the
                     ;; slice. (The probe-first path never reaches here for a
                     ;; missing key — it checks -blob-exists? first.)
                     (when (nil? (:data response))
                       (throw (store-key-not-found-ex key)))
                     (reset! fetched-object (:data response))
                     ;; Store ETag in bucket's cache for later use
                     (when (:etag response)
                       (reset! etag (:etag response))
                       (when-let [cache (:etag-cache bucket)]
                         (swap! cache assoc key (:etag response))))))
                 (Arrays/copyOfRange ^bytes @fetched-object (int 0) (int header-size)))))
  (-read-meta [_ meta-size env]
    (async+sync (:sync? env) io-sync-translation
                (io-try-
                 (Arrays/copyOfRange ^bytes @fetched-object (int header-size) (int (+ header-size meta-size))))))
  (-read-value [_ meta-size env]
    (async+sync (:sync? env) io-sync-translation
                (io-try-
                 (let [obj ^bytes @fetched-object]
                   (Arrays/copyOfRange obj (int (+ header-size meta-size)) (int (alength obj)))))))
  (-read-binary [_ meta-size locked-cb env]
    (async+sync (:sync? env) io-sync-translation
                (io-try-
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
    (async+sync (:sync? env) io-sync-translation
                (io-try- (delete client bucket (->key store-id store-key)))))
  (-blob-exists? [_ store-key env]
    (async+sync (:sync? env) io-sync-translation
                (io-try- (exists? client bucket (->key store-id store-key)))))
  (-copy [_ from to env]
    (async+sync (:sync? env) io-sync-translation
                (io-try- (copy client bucket (->key store-id from) (->key store-id to)))))
  (-atomic-move [_ from to env]
    (async+sync (:sync? env) io-sync-translation
                (io-try-
                 (copy client bucket (->key store-id from) (->key store-id to))
                 (delete client bucket (->key store-id from)))))
  (-migratable [_ _key _store-key env]
    (if (:sync? env) nil (go-try- nil)))
  (-migrate [_ _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-create-store [_ env]
    (async+sync (:sync? env) io-sync-translation
                (io-try-
                 (when-not (bucket-exists? client bucket)
                   (create-bucket client bucket))
                 ;; Write the marker object — this IS the store's registry
                 ;; entry. No shared registry object, so concurrent creates
                 ;; never contend.
                 (put-object client bucket (marker-key store-id) (.getBytes "konserve")))))
  (-store-exists? [_ env]
    (async+sync (:sync? env) io-sync-translation
                (io-try- (exists? client bucket (marker-key store-id)))))
  (-sync-store [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [_ env]
    (async+sync (:sync? env) io-sync-translation
                (io-try- (when (bucket-exists? client bucket)
                           (log/info :konserve.s3/delete-store "Deleting all konserve files. Use konserve-s3.core/delete-bucket to delete the bucket.")
                           (doseq [keys (->> (list-objects client bucket)
                                             (filter (fn [^String key]
                                                       (and (.startsWith key store-id)
                                                            (or (.endsWith key ".ksv")
                                                                (.endsWith key ".ksv.new")
                                                                (.endsWith key ".ksv.backup")
                                                                (.endsWith key ".konserve-metadata")))))
                                             (partition deletion-batch-size deletion-batch-size []))]
                             (log/trace :konserve.s3/deleting-keys {:keys keys})
                             (delete-keys client bucket keys))
                           ;; The marker object is included in the deletion
                           ;; filter above (it ends with .konserve-metadata),
                           ;; so removing it de-registers the store.
                           ;; The client is shared across stores (see
                           ;; s3-client) — do NOT close it here.
                           nil))))
  (-keys [_ env]
    (async+sync (:sync? env) io-sync-translation
                (io-try-
                 (let [keys (list-objects client bucket)]
                   (->> (filter (fn [^String key]
                                  (and (.startsWith key store-id)
                                       (or (.endsWith key ".ksv")
                                           (.endsWith key ".ksv.new")
                                           (.endsWith key ".ksv.backup"))))
                                keys)
                          ;; remove store-id prefix
                        (map #(subs % (inc (count store-id))))))))))

;; S3 reads are miss-safe: a GET on an absent key returns cleanly (get-object
;; catches the 404; -read-header throws store-key-not-found-ex), with no side
;; effect. So io-operation may skip its -blob-exists? probe and read directly —
;; one round trip (GET) per read instead of HEAD + GET.
(extend-type S3Bucket PReadMissSafe)

(defn connect-store
  "Connect a konserve store backed by S3."
  [s3-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        store-id (str (:id s3-spec))
        ;; The S3Client is shared (client-cache), so building a fresh backing
        ;; here is a cache lookup, not a client construction.
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
  "Historically closed the per-store S3Client. S3Clients are now shared across
   stores (see `s3-client` / `client-cache`), so per-store release is a no-op.
   Call `shutdown-clients!` on process exit to close the shared clients."
  [_store env]
  (async+sync (:sync? env) io-sync-translation
              (go-try- nil)))

(defn delete-store [s3-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        store-id (str (:id s3-spec))
        backing (S3Bucket. (s3-client s3-spec) (:bucket s3-spec) store-id (atom {}))]
    (-delete-store backing complete-opts)))

(defn list-stores
  "List all konserve stores in an S3 bucket by scanning per-store marker
   objects (<store-id>_.konserve-metadata). No central registry is kept, so
   concurrent store creation never contends on a shared object.

   Args:
     s3-spec - Map with :bucket, :region, :access-key, :secret, etc.
     opts - (optional) Runtime options map. Defaults to {:sync? true}

   Returns:
     Set of UUIDs representing store IDs in the bucket"
  [s3-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        client (s3-client s3-spec)              ;; shared — must not be closed
        bucket (:bucket s3-spec)
        suffix-len (count marker-suffix)]
    (async+sync (:sync? complete-opts) io-sync-translation
                (io-try-
                 ;; A non-existent bucket trivially holds no stores. list-objects
                 ;; throws NoSuchBucket (a 404) there; treat it as the empty set so
                 ;; list-stores is safe to call before any store/bucket exists.
                 (try
                   (->> (list-objects client bucket)
                        (keep (fn [^String k]
                                (when (.endsWith k marker-suffix)
                                  (try
                                    (UUID/fromString
                                     (subs k 0 (- (count k) suffix-len)))
                                    (catch IllegalArgumentException _ nil)))))
                        set)
                   (catch Exception e
                     (if (not-found? e) #{} (throw e))))))))

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
