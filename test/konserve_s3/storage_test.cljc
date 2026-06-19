(ns konserve-s3.storage-test
  "Fast, network-free tests for the shared pure helpers in storage.cljc. Runs on
   both the JVM (clojure.test) and Node (cljs.test / shadow :node-test)."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve-s3.storage :as st]))

(deftest key-naming
  (testing "->key joins store-id and key with an underscore"
    (is (= "s1_foo.ksv" (st/->key "s1" "foo.ksv"))))
  (testing "marker-key / registry-key"
    (is (= "s1_.konserve-metadata" (st/marker-key "s1")))
    (is (= "_konserve-stores-registry" st/registry-key))))

(deftest suffix-predicates
  (let [sid "abc"]
    (testing "data-key? matches blob suffixes for this store only"
      (is (st/data-key? sid "abc_foo.ksv"))
      (is (st/data-key? sid "abc_foo.ksv.new"))
      (is (st/data-key? sid "abc_foo.ksv.backup"))
      (is (not (st/data-key? sid "abc_.konserve-metadata")))
      (is (not (st/data-key? sid "other_foo.ksv")))
      (is (not (st/data-key? sid "abc_foo.txt"))))
    (testing "store-file? adds the metadata marker but excludes the registry"
      (is (st/store-file? sid "abc_foo.ksv"))
      (is (st/store-file? sid "abc_.konserve-metadata"))
      (is (not (st/store-file? sid st/registry-key))
          "the shared registry must never be deleted by delete-store"))))

(deftest registry-roundtrip
  (testing "serialize/deserialize round-trips a set of uuids"
    (let [ids #{#uuid "00000000-0000-0000-0000-000000000001"
                #uuid "00000000-0000-0000-0000-000000000002"}]
      (is (= ids (st/deserialize-registry (st/serialize-registry ids))))))
  (testing "nil bytes deserialize to the empty set"
    (is (= #{} (st/deserialize-registry nil)))))
