(ns konserve-s3.storage-test
  "Fast, network-free tests for the shared pure helpers in storage.cljc. Runs on
   both the JVM (clojure.test) and Node (cljs.test / shadow :node-test)."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve-s3.storage :as st]))

(deftest key-naming
  (testing "->key joins store-id and key with an underscore"
    (is (= "s1_foo.ksv" (st/->key "s1" "foo.ksv"))))
  (testing "marker-key appends the marker suffix"
    (is (= "_.konserve-metadata" st/marker-suffix))
    (is (= "s1_.konserve-metadata" (st/marker-key "s1")))
    (is (= (str "s1" st/marker-suffix) (st/marker-key "s1")))))

(deftest store-prefix-and-store-key
  (testing "store-prefix ends with the separator ->key inserts"
    (is (= "s1_" (st/store-prefix "s1")))
    (is (= (st/->key "s1" "foo.ksv") (str (st/store-prefix "s1") "foo.ksv"))))
  (testing "store-key strips the prefix"
    (is (= "foo.ksv" (st/store-key "s1" "s1_foo.ksv")))
    (is (= st/marker-store-key (st/store-key "s1" (st/marker-key "s1"))))
    (is (= ".konserve-metadata" st/marker-store-key)))
  (testing "store-key rejects another store's object"
    (is (nil? (st/store-key "s1" "s2_foo.ksv")))
    (testing "including a sibling whose id merely starts with ours"
      ;; `test` vs `test2`: a starts-with? on the bare store-id matched these.
      (is (nil? (st/store-key "test" "test2_foo.ksv"))))
    (testing "and a NESTED id, which even a prefixed listing returns"
      ;; `test_2`'s objects genuinely sit under `test_`. The store-key they
      ;; would yield (`2_foo.ksv`) is not junk: ->key maps it back onto
      ;; `test_2`'s real object, so `test` would read its neighbour's value.
      (is (nil? (st/store-key "test" "test_2_foo.ksv")))
      (is (= "foo.ksv" (st/store-key "test_2" "test_2_foo.ksv"))
          "while `test_2` still sees its own")
      (is (nil? (st/store-key "test" "test_2_.konserve-metadata"))
          "nor the nested store's marker")))
  (testing "a store-id ending in the separator keeps its own keys"
    (is (= "foo.ksv" (st/store-key "test_" "test__foo.ksv")))
    (is (nil? (st/store-key "test" "test__foo.ksv")))))

(deftest suffix-predicates
  (let [sid "abc"]
    (testing "data-key? matches blob suffixes for this store only"
      (is (st/data-key? sid "abc_foo.ksv"))
      (is (st/data-key? sid "abc_foo.ksv.new"))
      (is (st/data-key? sid "abc_foo.ksv.backup"))
      (is (not (st/data-key? sid "abc_.konserve-metadata")))
      (is (not (st/data-key? sid "other_foo.ksv")))
      (is (not (st/data-key? sid "abc_foo.txt"))))
    (testing "store-file? matches this store's blobs and marker only"
      (is (st/store-file? sid "abc_foo.ksv"))
      (is (st/store-file? sid "abc_.konserve-metadata"))
      (is (not (st/store-file? sid "other_.konserve-metadata"))
          "another store's marker is not this store's file")
      (is (not (st/store-file? sid "abc_foo.txt"))))
    (testing "neither predicate claims a sibling or nested store-id's objects"
      ;; -delete-store filters on store-file?, so a false positive here deletes
      ;; another store's data.
      (doseq [foreign ["abc2_foo.ksv" "abc2_.konserve-metadata"
                       "abc_2_foo.ksv" "abc_2_.konserve-metadata"]]
        (is (not (st/data-key? sid foreign)) foreign)
        (is (not (st/store-file? sid foreign)) foreign)))))
