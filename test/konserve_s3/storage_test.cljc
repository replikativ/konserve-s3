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
      (is (not (st/store-file? sid "abc_foo.txt"))))))
