(ns konserve-s3.io-executor-test
  (:require [clojure.core.async :refer [<!! go]]
            [clojure.test :refer [deftest is testing]]
            [konserve-s3.core :as s3])
  (:import [java.util.concurrent CountDownLatch TimeUnit]
           [java.util.concurrent.atomic AtomicInteger]))

(def ^:private expected-max-concurrency 64)

(deftest blocking-io-has-a-process-wide-concurrency-bound
  (testing "a burst queues work without creating one blocked thread per operation"
    (let [task-count (inc (* 3 expected-max-concurrency))
          active (AtomicInteger.)
          peak (AtomicInteger.)
          at-capacity (CountDownLatch. expected-max-concurrency)
          over-limit-started (CountDownLatch. (inc expected-max-concurrency))
          release (CountDownLatch. 1)
          task (fn []
                 (let [n (.incrementAndGet active)]
                   (.accumulateAndGet peak n max)
                   (.countDown at-capacity)
                   (.countDown over-limit-started)
                   (try
                     (.await release 10 TimeUnit/SECONDS)
                     :done
                     (finally
                       (.decrementAndGet active)))))
          results (mapv (fn [_] (#'s3/io-task-ch task)) (range task-count))]
      (try
        (is (true? (.await at-capacity 5 TimeUnit/SECONDS))
            "the executor must still make 64-way progress")
        (is (false? (.await over-limit-started 1 TimeUnit/SECONDS))
            "operation 65 must remain queued while the first 64 are blocked")
        (is (= :dispatch-alive (<!! (go :dispatch-alive)))
            "saturating S3 I/O must not starve core.async dispatch")
        (finally
          (.countDown release)))
      (is (every? #{:done} (mapv <!! results)))
      (is (<= (.get peak) expected-max-concurrency)
          (str "observed " (.get peak) " concurrent blocking operations"))
      (is (zero? (.get active))))))
