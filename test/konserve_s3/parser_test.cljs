(ns konserve-s3.parser-test
  "Fast, network-free tests for the cljs-only pure logic in core.cljs: the
   ListObjectsV2 XML parser. Runs under shadow :node-test."
  (:require [cljs.test :refer [deftest is testing]]
            [konserve-s3.core :as s3]))

(deftest parse-list-xml-keys
  (testing "extracts <Key> values and decodes entities"
    (let [xml (str "<?xml version=\"1.0\"?>"
                   "<ListBucketResult>"
                   "<Contents><Key>s1_a.ksv</Key></Contents>"
                   "<Contents><Key>s1_b&amp;c.ksv</Key></Contents>"
                   "<IsTruncated>false</IsTruncated>"
                   "</ListBucketResult>")
          {:keys [keys truncated? next-token]} (s3/parse-list-xml xml)]
      (is (= ["s1_a.ksv" "s1_b&c.ksv"] keys))
      (is (false? truncated?))
      (is (nil? next-token)))))

(deftest parse-list-xml-pagination
  (testing "captures truncation flag and continuation token"
    (let [xml (str "<ListBucketResult>"
                   "<Contents><Key>k1</Key></Contents>"
                   "<IsTruncated>true</IsTruncated>"
                   "<NextContinuationToken>tok-123==</NextContinuationToken>"
                   "</ListBucketResult>")
          {:keys [keys truncated? next-token]} (s3/parse-list-xml xml)]
      (is (= ["k1"] keys))
      (is (true? truncated?))
      (is (= "tok-123==" next-token)))))

(deftest parse-list-xml-empty
  (testing "an empty listing yields no keys and is not truncated"
    (let [{:keys [keys truncated? next-token]}
          (s3/parse-list-xml "<ListBucketResult></ListBucketResult>")]
      (is (= [] keys))
      (is (false? truncated?))
      (is (nil? next-token)))))
