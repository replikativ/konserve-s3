(ns konserve-s3.core-test
  (:require [clojure.test :refer [deftest testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-s3.core :refer [connect-store release delete-store]]))

(def s3-spec {:region "us-west-1"
              :store-id "test-store"})

(deftest s3-compliance-sync-test
  (let [s3-spec (assoc s3-spec :bucket "konserve-s3-sync-test")
        _     (delete-store s3-spec :opts {:sync? true})
        store (connect-store s3-spec :opts {:sync? true})]
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (delete-store s3-spec :opts {:sync? true})))

(deftest s3-compliance-async-test
  (let [s3-spec (assoc s3-spec :bucket "konserve-s3-async-test")
        _     (<!! (delete-store s3-spec :opts {:sync? false}))
        store (<!! (connect-store s3-spec :opts {:sync? false}))]
    (testing "Compliance test with asynchronous store"
      (compliance-test store))
    (<!! (release store {:sync? false}))
    (<!! (delete-store s3-spec :opts {:sync? false}))))

