(ns konserve-s3.storage
  "Platform-agnostic helpers shared by the JVM (core.clj) and ClojureScript
   (core.cljs) S3 backends: konserve key naming, the store-file suffix
   predicates, and registry (de)serialization.

   Everything here is pure and free of S3 I/O, so both backends build on a
   single source of truth. See PLAN-konserve-s3-cljs.md."
  (:require [clojure.string :as str]
            #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])))

;; --- konserve key naming -----------------------------------------------------

(defn ->key
  "S3 object key for konserve `key` within `store-id`."
  [store-id key]
  (str store-id "_" key))

(defn marker-key
  "S3 key for the per-store metadata marker that makes a store discoverable
   and backs `-store-exists?`."
  [store-id]
  (str store-id "_.konserve-metadata"))

(def registry-key
  "S3 key of the central registry object listing all store ids in a bucket."
  "_konserve-stores-registry")

;; --- store-file predicates ---------------------------------------------------

(def ^:const ksv-suffixes
  "Suffixes of konserve blob objects (the live blob plus its copy/move temps)."
  [".ksv" ".ksv.new" ".ksv.backup"])

(defn data-key?
  "True when `key` is a konserve blob (.ksv / .ksv.new / .ksv.backup) belonging
   to `store-id`. Used to enumerate a store's keys."
  [store-id key]
  (and (str/starts-with? key store-id)
       (boolean (some #(str/ends-with? key %) ksv-suffixes))))

(defn store-file?
  "True when `key` is any object konserve owns for `store-id`: a blob or the
   metadata marker. Used to scope deletion in `-delete-store` (the shared
   registry object is intentionally excluded)."
  [store-id key]
  (and (str/starts-with? key store-id)
       (or (data-key? store-id key)
           (str/ends-with? key ".konserve-metadata"))))

;; --- registry (de)serialization ----------------------------------------------

(defn serialize-registry
  "Serialize a collection of store-id UUIDs to UTF-8 bytes (a pr-str'd vector)."
  [store-ids]
  (let [s (pr-str (vec store-ids))]
    #?(:clj  (.getBytes ^String s "UTF-8")
       :cljs (.encode (js/TextEncoder.) s))))

(defn deserialize-registry
  "Deserialize registry bytes back to a set of store-id UUIDs; nil -> #{}."
  [data]
  (if data
    (let [s #?(:clj  (String. ^bytes data "UTF-8")
               :cljs (.decode (js/TextDecoder.) data))]
      (set (edn/read-string s)))
    #{}))
