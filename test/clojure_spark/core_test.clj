(ns clojure-spark.core-test
  (:require [clojure.test :refer :all]
            [clojure-spark.utils :as utils]))

(deftest build-columns-test
  (testing "build-columns does not require starting Spark"
    (is (= 2 (count (utils/build-columns "supplier" "product_name"))))))
