(ns konserve-s3.parser-test
  "Fast, network-free tests for the cljs-only pure logic in core.cljs: the
   ListObjectsV2 XML parser, and the REQUEST the backing store makes to enumerate
   a store. Runs under shadow :node-test (and the browser builds, being
   network-free)."
  (:require [cljs.test :refer [deftest is testing async]]
            [clojure.string :as str]
            [clojure.core.async :refer [go <!] :include-macros true]
            [konserve.impl.storage-layout :as storage-layout]
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

;; --- what the backing store ASKS S3 for -------------------------------------

(defn- spy-conn
  "A conn whose client records every URL fetched and answers with an empty
   ListObjectsV2 result. Lets the request be asserted without a bucket."
  [requested]
  {:client      #js {:fetch (fn [url _opts]
                              (swap! requested conj url)
                              (js/Promise.resolve
                               #js {:ok   true
                                    :text (fn []
                                            (js/Promise.resolve
                                             "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"))}))}
   :endpoint    "http://s3.test"
   :bucket      "b"
   :path-style? true})

(deftest enumeration-is-scoped-to-the-store-prefix
  (testing "-keys and -delete-store ask for `<store-id>_`, not the bare store-id"
    ;; The bare store-id is a prefix of every sibling that starts with it, so
    ;; listing under it over-fetches `p2_*` and `p_2_*` — the cost this scoping
    ;; exists to avoid. The client-side predicates discard those objects either
    ;; way, so no assertion about the RESULT can see the difference: the request
    ;; is the only place it shows.
    (async done
           (let [requested (atom [])
                 backing   (s3/->S3BackingStore (spy-conn requested) "p" (atom {}))]
             (go
               (<! (storage-layout/-keys backing {:sync? false}))
               (is (= 1 (count @requested)))
               (is (str/includes? (first @requested) "prefix=p_")
                   "-keys must scope its listing to this store")
               (reset! requested [])
               (<! (storage-layout/-delete-store backing {:sync? false}))
               (is (= 1 (count @requested)))
               (is (str/includes? (first @requested) "prefix=p_")
                   "-delete-store must scope its listing too")
               (is (not-any? #(str/includes? % "prefix=p&") @requested)
                   "and neither may list under the bare store-id")
               (done))))))
