(ns konserve-s3.core
  "ClojureScript konserve backend for S3-compatible object storage (Amazon S3,
   Cloudflare R2, MinIO, ...). The cljs sibling of the JVM backend in core.clj.

   This namespace has two layers, mirroring core.clj:

   1. Low-level S3 REST operations, signed with aws4fetch and returning
      core.async channels. Every operation yields a promise-chan carrying
      either the result or a js/Error / ex-info; konserve's `<?-` rethrows the
      latter. This is the cljs half of the platform S3-ops abstraction in
      PLAN-konserve-s3-cljs.md.

   2. The konserve backend itself: the `S3Blob` / `S3BackingStore` records
      implementing konserve.impl.storage-layout, plus `connect-s3-store` /
      `delete-s3-store` / `list-stores` and the konserve.store `:s3`
      multimethods. Async only (`:sync? false`), like the IndexedDB backend.

   Provider-neutral: the same code talks to every S3-compatible API; only the
   `:endpoint`/`:region`/`:path-style?` config differs (see `connect` and PLAN
   \"Provider specifics\")."
  (:require [clojure.core.async :refer [put! close! take! go <! timeout] :include-macros true]
            [konserve.impl.defaults :refer [connect-default-store]]
            [konserve.impl.storage-layout :as storage-layout
             :refer [PBackingStore PBackingBlob PBackingLock header-size]]
            [konserve.store :as store]
            [konserve.compressor]
            [konserve.encryptor]
            [konserve-s3.storage :refer [->key marker-key registry-key
                                         serialize-registry deserialize-registry
                                         data-key? store-file?]]
            [superv.async :refer [go-try- <?-] :include-macros true]
            [konserve.utils :refer-macros [with-promise]]
            ["aws4fetch" :refer [AwsClient]]))

;; --- client / connection -----------------------------------------------------

(defn make-client
  "Construct an aws4fetch AwsClient from a spec map. `:region` defaults to
   \"auto\" (correct for R2; pass the real region for Amazon S3)."
  [{:keys [access-key secret region session-token]}]
  (AwsClient. #js {:accessKeyId     access-key
                   :secretAccessKey secret
                   :sessionToken    session-token
                   :service         "s3"
                   :region          (or region "auto")}))

(defn- split-scheme
  "\"https://host\" -> [\"https://\" \"host\"]; defaults the scheme to https."
  [endpoint]
  (let [idx (.indexOf endpoint "://")]
    (if (neg? idx)
      ["https://" endpoint]
      [(subs endpoint 0 (+ idx 3)) (subs endpoint (+ idx 3))])))

(defn connect
  "Build a connection map from an S3 spec. Required keys: `:endpoint` (service
   endpoint, no bucket), `:bucket`, `:access-key`, `:secret`. Optional:
   `:region`, `:session-token`, `:path-style?` (default true — works for R2,
   MinIO and Amazon S3; set false for Amazon virtual-hosted addressing)."
  [{:keys [endpoint bucket path-style?] :or {path-style? true} :as spec}]
  {:client      (make-client spec)
   :endpoint    endpoint
   :bucket      bucket
   :path-style? path-style?})

(defn bucket-url
  "URL of the bucket root (used for ListObjectsV2)."
  [{:keys [endpoint bucket path-style?]}]
  (if path-style?
    (str endpoint "/" bucket)
    (let [[scheme host] (split-scheme endpoint)]
      (str scheme bucket "." host))))

(defn object-url
  "URL of a single object. konserve keys are restricted to UUID hex plus
   `_`/`.`/`-` and the registry/marker names, all URL-safe, so no extra
   percent-encoding is applied (which would otherwise be double-encoded by
   aws4fetch's SigV4 canonicalization)."
  [conn key]
  (str (bucket-url conn) "/" key))

(defn- etag
  "Strong/weak ETag from a fetch Response, or nil. Requires the bucket CORS
   policy to expose the ETag header in the browser (see PLAN)."
  [resp]
  (.. resp -headers (get "etag")))

;; --- operations --------------------------------------------------------------

(defn get-object
  "GET an object. Yields {:data Uint8Array :etag string} on success, nil when
   the object does not exist (404), or an ex-info on error."
  [conn key]
  (with-promise out
    ;; :cache "no-store" is essential in the browser: without it the HTTP cache
    ;; can serve a stale body/ETag, breaking read-after-write consistency and
    ;; the ETag-based optimistic locking (a stale ETag makes every conditional
    ;; PUT 412 forever). No-op under Node's fetch.
    (-> (.fetch (:client conn) (object-url conn key) #js {:cache "no-store"})
        (.then (fn [resp]
                 (cond
                   (== 404 (.-status resp)) (close! out)
                   (not (.-ok resp))
                   (put! out (ex-info "S3 get-object failed"
                                      {:status (.-status resp) :key key}))
                   :else
                   (-> (.arrayBuffer resp)
                       (.then (fn [ab]
                                (put! out {:data (js/Uint8Array. ab)
                                           :etag (etag resp)})))
                       (.catch (fn [e]
                                 (put! out (ex-info "S3 get-object: error reading body"
                                                    {:key key :cause e}))))))))
        (.catch (fn [e]
                  (put! out (ex-info "S3 get-object network error"
                                     {:key key :cause e})))))))

(defn put-object
  "PUT an object with body `bytes` (a Uint8Array). Optional conditional headers
   `:if-match` / `:if-none-match`. Yields the new ETag (or true) on success,
   `::conflict` on a 412 precondition failure, or an ex-info on error."
  [conn key bytes & {:keys [if-match if-none-match]}]
  (with-promise out
    (let [headers #js {}]
      (when if-match      (aset headers "if-match" if-match))
      (when if-none-match (aset headers "if-none-match" if-none-match))
      (-> (.fetch (:client conn) (object-url conn key)
                  #js {:method "PUT" :body bytes :headers headers :cache "no-store"})
          (.then (fn [resp]
                   (cond
                     (== 412 (.-status resp)) (put! out ::conflict)
                     (not (.-ok resp))
                     (put! out (ex-info "S3 put-object failed"
                                        {:status (.-status resp) :key key}))
                     :else (put! out (or (etag resp) true)))))
          (.catch (fn [e]
                    (put! out (ex-info "S3 put-object network error"
                                       {:key key :cause e}))))))))

(defn delete-object
  "DELETE an object. Treats 404 as success. Yields nil on success, ex-info on
   error."
  [conn key]
  (with-promise out
    (-> (.fetch (:client conn) (object-url conn key) #js {:method "DELETE" :cache "no-store"})
        (.then (fn [resp]
                 (if (or (.-ok resp) (== 404 (.-status resp)))
                   (close! out)
                   (put! out (ex-info "S3 delete-object failed"
                                      {:status (.-status resp) :key key})))))
        (.catch (fn [e]
                  (put! out (ex-info "S3 delete-object network error"
                                     {:key key :cause e})))))))

(defn copy-object
  "Server-side copy via CopyObject (PUT with x-amz-copy-source). Yields nil on
   success, ex-info on error."
  [conn from-key to-key]
  (with-promise out
    (let [src (str "/" (:bucket conn) "/" from-key)]
      (-> (.fetch (:client conn) (object-url conn to-key)
                  #js {:method "PUT" :headers #js {"x-amz-copy-source" src} :cache "no-store"})
          (.then (fn [resp]
                   (if (.-ok resp)
                     (close! out)
                     (put! out (ex-info "S3 copy-object failed"
                                        {:status (.-status resp)
                                         :from from-key :to to-key})))))
          (.catch (fn [e]
                    (put! out (ex-info "S3 copy-object network error"
                                       {:from from-key :to to-key :cause e}))))))))

(defn head-object
  "HEAD an object. Yields true if it exists, false on 404, ex-info on error."
  [conn key]
  (with-promise out
    (-> (.fetch (:client conn) (object-url conn key) #js {:method "HEAD" :cache "no-store"})
        (.then (fn [resp]
                 (cond
                   (.-ok resp)              (put! out true)
                   (== 404 (.-status resp)) (put! out false)
                   :else (put! out (ex-info "S3 head-object failed"
                                            {:status (.-status resp) :key key})))))
        (.catch (fn [e]
                  (put! out (ex-info "S3 head-object network error"
                                     {:key key :cause e})))))))

(defn- decode-entities
  "Decode the handful of XML entities ListObjectsV2 can emit in <Key> text."
  [s]
  (-> s
      (.replaceAll "&amp;" "&")
      (.replaceAll "&lt;" "<")
      (.replaceAll "&gt;" ">")
      (.replaceAll "&quot;" "\"")
      (.replaceAll "&#39;" "'")))

(defn parse-list-xml
  "Parse a ListObjectsV2 XML response without an XML library (no DOMParser in
   Node): pull out <Key> values plus the truncation/continuation markers. Only
   these fields are needed (see PLAN)."
  [xml]
  {:keys       (->> (re-seq #"<Key>([\s\S]*?)</Key>" xml)
                    (map (comp decode-entities second))
                    vec)
   :truncated? (boolean (re-find #"<IsTruncated>\s*true\s*</IsTruncated>" xml))
   :next-token (some-> (re-find #"<NextContinuationToken>([\s\S]*?)</NextContinuationToken>" xml)
                       second)})

(defn list-objects
  "ListObjectsV2 under `prefix`, following continuation tokens. Yields a vector
   of object keys, or an ex-info on error."
  ([conn prefix] (list-objects conn prefix nil []))
  ([conn prefix continuation-token acc]
   (with-promise out
     (let [url (str (bucket-url conn) "?list-type=2"
                    (when (seq prefix)
                      (str "&prefix=" (js/encodeURIComponent prefix)))
                    (when continuation-token
                      (str "&continuation-token="
                           (js/encodeURIComponent continuation-token))))]
       (-> (.fetch (:client conn) url #js {:cache "no-store"})
           (.then (fn [resp]
                    (if-not (.-ok resp)
                      (put! out (ex-info "S3 list-objects failed"
                                         {:status (.-status resp) :prefix prefix}))
                      (-> (.text resp)
                          (.then (fn [xml]
                                   (let [{:keys [keys truncated? next-token]}
                                         (parse-list-xml xml)
                                         acc' (into acc keys)]
                                     (if truncated?
                                       (take! (list-objects conn prefix next-token acc')
                                              #(put! out %))
                                       (put! out acc')))))
                          (.catch (fn [e]
                                    (put! out (ex-info "S3 list-objects: error reading body"
                                                       {:prefix prefix :cause e}))))))))
           (.catch (fn [e]
                     (put! out (ex-info "S3 list-objects network error"
                                        {:prefix prefix :cause e})))))))))

;; =============================================================================
;; Backend: byte helpers
;; =============================================================================

(defn- ->u8
  "Coerce an array-like (Uint8Array, ArrayBuffer, fress' Int8Array, ...) to a
   Uint8Array view without copying when possible."
  [x]
  (cond
    (instance? js/Uint8Array x)  x
    (instance? js/ArrayBuffer x) (js/Uint8Array. x)
    (and (.-buffer x) (.-byteLength x))
    (js/Uint8Array. (.-buffer x) (.-byteOffset x) (.-byteLength x))
    :else (js/Uint8Array.from x)))

(defn- concat-bytes
  "Concatenate array-likes into one Uint8Array (header ‖ meta ‖ value)."
  [parts]
  (let [parts (map ->u8 parts)
        total (reduce + (map #(.-length %) parts))
        out   (js/Uint8Array. total)]
    (reduce (fn [offset a] (.set out a offset) (+ offset (.-length a))) 0 parts)
    out))

;; =============================================================================
;; Backend: stores registry (mirrors core.clj update-registry, async via ETags)
;; =============================================================================

(defn- update-registry
  "Update the central stores registry with optimistic concurrency control via
   ETags. `modify-fn` takes the current set of store-id UUIDs and returns the
   new set. Yields the new set, or throws after `max-retries` conflicts.

   All writers share the single registry object, so connect (which re-adds an
   already-listed store id on every call, via connect-default-store's
   -create-store) is a common contention point. Two things keep that in check:
   idempotent updates (new-set = current-set) skip the write entirely, and
   genuine conflicts back off with jitter before retrying so concurrent writers
   de-synchronise."
  ([conn modify-fn] (update-registry conn modify-fn 10))
  ([conn modify-fn max-retries]
   (go-try-
    (loop [attempt 0]
      (let [current     (<?- (get-object conn registry-key))
            current-set (deserialize-registry (:data current))
            new-set     (modify-fn current-set)]
        (if (= new-set current-set)
          ;; Nothing changed (e.g. reconnecting to a store already listed) —
          ;; don't touch the shared registry, so concurrent connects don't
          ;; contend or exhaust retries.
          new-set
          (let [new-data (serialize-registry new-set)
                res      (<?- (if current
                               (put-object conn registry-key new-data :if-match (:etag current))
                               (put-object conn registry-key new-data :if-none-match "*")))]
            (if (= res ::conflict)
              (if (< attempt max-retries)
                (do
                  ;; Randomised backoff, widening with each attempt, to spread
                  ;; out contending writers.
                  (<! (timeout (+ 5 (rand-int (* 10 (inc attempt))))))
                  (recur (inc attempt)))
                (throw (ex-info "Registry update failed after max retries"
                                {:max-retries max-retries})))
              new-set))))))))

;; =============================================================================
;; Backend: records implementing konserve.impl.storage-layout
;; =============================================================================

(defrecord S3Blob [conn key data fetched etag-cache]
  PBackingBlob
  (-get-lock [_ _env]
    ;; No-op lock: S3 has no blob lock; concurrency safety comes from the
    ;; ETag-based optimistic locking in -sync. Must not return nil (see
    ;; defaults/io-operation), so we return a real PBackingLock.
    (go (reify PBackingLock (-release [_ _env] (go nil)))))
  (-sync [_ env]
    ;; -sync runs on a *fresh* blob created by defaults/update-blob, distinct
    ;; from the one -read-header populated; so the read ETag lives in the
    ;; store-wide `etag-cache` (keyed by object key), mirroring the JVM
    ;; backend's bucket cache. Gated on :optimistic-locking-retries so the
    ;; default is plain last-write-wins PUTs, like core.clj.
    (let [{:keys [header meta value]} @data
          retries     (get-in env [:config :optimistic-locking-retries] 0)
          cached-etag (when (pos? retries) (get @etag-cache key))]
      (with-promise out
        (if-not (and header meta value)
          (put! out (ex-info "Updating a row is only possible if header, meta and value are set."
                             {:data @data}))
          (let [bytes  (concat-bytes [header meta value])
                put-ch (if cached-etag
                         (put-object conn key bytes :if-match cached-etag)
                         (put-object conn key bytes))]
            (take! put-ch
                   (fn [res]
                     (cond
                       (instance? js/Error res) (put! out res)
                       (= res ::conflict)
                       (put! out (ex-info "Optimistic lock conflict"
                                          {:type :optimistic-lock-conflict
                                           :key key :etag cached-etag}))
                       :else (do (reset! data {})
                                 (swap! etag-cache dissoc key)
                                 (reset! fetched nil)
                                 (close! out))))))))))
  (-close [_ _env] (go nil))
  (-read-header [_ _env]
    (with-promise out
      (if-let [f @fetched]
        (put! out (.slice f 0 header-size))
        (take! (get-object conn key)
               (fn [res]
                 (cond
                   (instance? js/Error res) (put! out res)
                   (nil? res) (put! out (ex-info "S3 blob not found while reading header"
                                                 {:key key}))
                   :else (do (reset! fetched (:data res))
                             ;; Stash the ETag store-wide so the later -sync (a
                             ;; different blob instance) can do a conditional PUT.
                             (when (:etag res)
                               (swap! etag-cache assoc key (:etag res)))
                             (put! out (.slice (:data res) 0 header-size)))))))))
  (-read-meta [_ meta-size _env]
    (go (.slice @fetched header-size (+ header-size meta-size))))
  (-read-value [_ meta-size _env]
    (go (.slice @fetched (+ header-size meta-size))))
  (-read-binary [_ meta-size locked-cb _env]
    (with-promise out
      (let [blob (js/Blob. #js [@fetched])]
        (take! (locked-cb {:input-stream (.stream blob)
                           :size         (.-size blob)
                           :offset       (+ meta-size header-size)})
               #(put! out %)))))
  (-write-header [_ header _env] (go (swap! data assoc :header header)))
  (-write-meta   [_ meta _env]   (go (swap! data assoc :meta meta)))
  (-write-value  [_ value _meta-size _env] (go (swap! data assoc :value value)))
  (-write-binary [_ _meta-size blob _env]  (go (swap! data assoc :value blob))))

(defrecord S3BackingStore [conn store-id etag-cache]
  PBackingStore
  (-create-blob [_ store-key _env]
    ;; The store-wide etag-cache is shared with every blob so a read's ETag
    ;; survives into the separate write-blob's -sync (see S3Blob).
    (go (->S3Blob conn (->key store-id store-key) (atom {}) (atom nil) etag-cache)))
  (-delete-blob [_ store-key _env]
    (delete-object conn (->key store-id store-key)))
  (-blob-exists? [_ store-key _env]
    (head-object conn (->key store-id store-key)))
  (-copy [_ from to _env]
    (copy-object conn (->key store-id from) (->key store-id to)))
  (-atomic-move [_ from to _env]
    ;; S3 has no rename: CopyObject + DeleteObject (mirrors core.clj).
    (with-promise out
      (take! (copy-object conn (->key store-id from) (->key store-id to))
             (fn [res]
               (if (instance? js/Error res)
                 (put! out res)
                 (take! (delete-object conn (->key store-id from))
                        #(put! out %)))))))
  (-migratable [_ _key _store-key _env] (go nil))
  (-migrate [_ _migration-key _key-vec _serializer _read-handlers _write-handlers _env]
    (go nil))
  (-handle-foreign-key [_ _migration-key _serializer _read-handlers _write-handlers _env]
    (go []))
  (-create-store [_ _env]
    ;; Assumes the bucket already exists. Writes the per-store marker and adds
    ;; the store-id to the registry; both are idempotent (connect calls this
    ;; unconditionally).
    (go-try-
     (<?- (put-object conn (marker-key store-id)
                      (.encode (js/TextEncoder.) "konserve")))
     (<?- (update-registry conn #(conj % (uuid store-id))))
     nil))
  (-store-exists? [_ _env]
    (head-object conn (marker-key store-id)))
  (-sync-store [_ _env] (go nil))
  (-delete-store [_ _env]
    (go-try-
     (let [all-keys  (<?- (list-objects conn store-id))
           to-delete (filter #(store-file? store-id %) all-keys)]
       ;; Delete one-by-one (REST batch delete needs a signed XML POST; the
       ;; per-store key count is small). The shared registry is excluded by
       ;; store-file?.
       (loop [[k & more] to-delete]
         (when k
           (<?- (delete-object conn k))
           (recur more)))
       (<?- (update-registry conn #(disj % (uuid store-id))))
       nil)))
  (-keys [_ _env]
    (go-try-
     (let [all-keys (<?- (list-objects conn store-id))]
       (->> all-keys
            (filter #(data-key? store-id %))
            ;; strip the "{store-id}_" prefix, keeping the .ksv* suffix
            (map #(subs % (inc (count store-id)))))))))

;; =============================================================================
;; Backend: public API
;; =============================================================================

(defn connect-s3-store
  "Connect to (creating if absent) an S3-backed konserve store. Async only.

   `s3-spec` keys:
     :endpoint    service endpoint, e.g. \"https://s3.us-west-1.amazonaws.com\"
                  or \"https://<account>.r2.cloudflarestorage.com\"
     :bucket      bucket name (must already exist)
     :access-key  / :secret    credentials
     :id          store id, a UUID string
     :region      (optional, default \"auto\"; use the real region for Amazon S3)
     :path-style? (optional, default true)
     :session-token (optional)
     :config      (optional) konserve store config overrides

   Returns a channel yielding the store. Release is a no-op (stateless fetch)."
  [s3-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? false} opts)
        store-id      (str (:id s3-spec))
        backing       (->S3BackingStore (connect s3-spec) store-id (atom {}))
        config        {:opts               complete-opts
                       :config             (merge {:sync-blob? true
                                                   :in-place?  true
                                                   :no-backup? true
                                                   :lock-blob? true}
                                                  (:config s3-spec))
                       :default-serializer :FressianSerializer
                       :buffer-size        (* 1024 1024)}]
    (connect-default-store backing config)))

(defn delete-s3-store
  "Delete all konserve objects for a store (and its registry entry). Does not
   delete the bucket. Async only; yields a channel."
  [s3-spec & {:keys [opts]}]
  (let [store-id (str (:id s3-spec))
        backing  (->S3BackingStore (connect s3-spec) store-id (atom {}))]
    (storage-layout/-delete-store backing (merge {:sync? false} opts))))

(defn list-stores
  "List all konserve store ids in a bucket by reading the central registry.
   Yields a channel carrying a set of UUIDs."
  [s3-spec & {:keys [opts]}]
  (let [conn (connect s3-spec)]
    (go-try-
     (deserialize-registry (:data (<?- (get-object conn registry-key)))))))

;; =============================================================================
;; Multimethod registration for konserve.store dispatch
;; =============================================================================

(defmethod store/-connect-store :s3
  [{:keys [bucket id] :as config} opts]
  (assert (false? (:sync? opts)) "S3 store connections must be async (set :sync? to false)")
  (go-try-
   (let [store-id (str id)
         backing  (->S3BackingStore (connect config) store-id (atom {}))
         exists   (<?- (storage-layout/-store-exists? backing opts))]
     (when-not exists
       (throw (ex-info (str "S3 store does not exist: " bucket "/" store-id)
                       {:bucket bucket :store-id store-id})))
     (<?- (connect-s3-store config :opts opts)))))

(defmethod store/-create-store :s3
  [{:keys [bucket id] :as config} opts]
  (assert (false? (:sync? opts)) "S3 store creation must be async (set :sync? to false)")
  (go-try-
   (let [store-id (str id)
         backing  (->S3BackingStore (connect config) store-id (atom {}))
         exists   (<?- (storage-layout/-store-exists? backing opts))]
     (when exists
       (throw (ex-info (str "S3 store already exists: " bucket "/" store-id)
                       {:bucket bucket :store-id store-id})))
     (<?- (connect-s3-store config :opts opts)))))

(defmethod store/-store-exists? :s3
  [config opts]
  (assert (false? (:sync? opts)) "S3 store existence checks must be async (set :sync? to false)")
  (storage-layout/-store-exists? (->S3BackingStore (connect config) (str (:id config)) (atom {})) opts))

(defmethod store/-delete-store :s3
  [config opts]
  (delete-s3-store config :opts opts))

(defmethod store/-release-store :s3
  [_config _store _opts]
  (go nil))
