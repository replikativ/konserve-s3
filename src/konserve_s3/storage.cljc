(ns konserve-s3.storage
  "Platform-agnostic helpers shared by the JVM (core.clj) and ClojureScript
   (core.cljs) S3 backends: konserve key naming and the store-file suffix
   predicates.

   Everything here is pure and free of S3 I/O, so both backends build on a
   single source of truth."
  (:require [clojure.string :as str]))

;; --- konserve key naming -----------------------------------------------------

(defn ->key
  "S3 object key for konserve `key` within `store-id`."
  [store-id key]
  (str store-id "_" key))

(def marker-suffix
  "Suffix of the per-store metadata marker object. Every store has exactly one
   marker; the set of marker objects IS the store registry (see each backend's
   `list-stores`) — there is no central registry object to contend on."
  "_.konserve-metadata")

(defn marker-key
  "S3 key for the per-store metadata marker that makes a store discoverable
   and backs `-store-exists?`."
  [store-id]
  (str store-id marker-suffix))

(defn store-prefix
  "S3 key prefix shared by EVERY object of `store-id` — its blobs (`->key`) and
   its marker (`marker-key`, whose suffix also starts with `_`).

   Pass it to a backend's `list-objects` so S3 returns one store's objects
   instead of the whole bucket: a bucket may hold thousands of stores, and
   listing it whole to keep a fraction of a percent of the keys costs one
   request per 1000 objects IN THE BUCKET, on every call, growing with every
   unrelated store added. Only a genuinely bucket-wide question (`list-stores`,
   which has to find every marker) should list unprefixed."
  [store-id]
  (str store-id "_"))

(def marker-store-key
  "What `marker-suffix` looks like once the store prefix is stripped."
  (subs marker-suffix 1))

(defn store-key
  "The konserve store-key that `key` names for `store-id` — `key` with the
   store's prefix stripped — or nil when `key` is not this store's object.

   Two ways a sibling store's object can look like ours, both of which this
   rejects and a `starts-with?` on the bare store-id does not:

     `test2_<uuid>.ksv`   — shares a prefix with `test` without being nested
                            under it. Excluded by requiring the `_`.
     `test_2_<uuid>.ksv`  — genuinely IS under `test`'s `test_` prefix, so even
                            a prefixed listing returns it. The store-key it
                            would yield, `2_<uuid>.ksv`, is not junk that later
                            fails to read: `->key` maps it straight back onto
                            `test_2`'s real object, so `test` would read its
                            neighbour's value — and `-delete-store` on `test`
                            would delete it.

   What separates them is the `_`: konserve store-keys are
   `(str (uuid key) \".ksv\")` — a UUID plus suffix, never an underscore (see
   `konserve.impl.defaults/key->store-key`) — and the marker's store-key is
   `.konserve-metadata`. `->key` is the only thing that introduces one, so a
   remainder containing `_` came from a deeper store-id, never from a key of
   ours. store-id is `(str (:id s3-spec))`, i.e. any string, so this is
   reachable without unusual setup; UUID ids are structurally immune."
  [store-id key]
  (let [prefix (store-prefix store-id)]
    (when (str/starts-with? key prefix)
      (let [store-key (subs key (count prefix))]
        (when-not (str/includes? store-key "_")
          store-key)))))

;; --- store-file predicates ---------------------------------------------------

(def ^:const ksv-suffixes
  "Suffixes of konserve blob objects (the live blob plus its copy/move temps)."
  [".ksv" ".ksv.new" ".ksv.backup"])

(defn data-key?
  "True when `key` is a konserve blob (.ksv / .ksv.new / .ksv.backup) belonging
   to `store-id` — and not to a sibling or nested store-id (see `store-key`).
   Used to enumerate a store's keys."
  [store-id key]
  (boolean (when-let [store-key (store-key store-id key)]
             (some #(str/ends-with? store-key %) ksv-suffixes))))

(defn store-file?
  "True when `key` is any object konserve owns for `store-id`: a blob or the
   metadata marker. Used to scope deletion in `-delete-store` — where mistaking
   a nested store-id's object for our own deletes someone else's data."
  [store-id key]
  (boolean (when-let [store-key (store-key store-id key)]
             (or (some #(str/ends-with? store-key %) ksv-suffixes)
                 (= store-key marker-store-key)))))
