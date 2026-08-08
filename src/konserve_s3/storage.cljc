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
   metadata marker. Used to scope deletion in `-delete-store`."
  [store-id key]
  (and (str/starts-with? key store-id)
       (or (data-key? store-id key)
           (str/ends-with? key marker-suffix))))
