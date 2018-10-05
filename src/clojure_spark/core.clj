(ns clojure-spark.core
  (:require [clojure-spark.query-to-map :as qm]
            [clojure.pprint :as p]
            [clojure-spark.utils :as util])
  (:gen-class))

(defn -main [& args]
  (util/build-spark-context "welcome")
    (p/pprint
     (qm/process-input-json-file
      "resources/input.json"
      "suppliers"
      (str "select supplier, product_name , avg(quantity) as avg_qty ,"
           " avg(unit_cost) as avg_cost from suppliers "
           " group by supplier, product_name ")))

  (p/pprint
   (qm/process-input-parquet-file
    (util/str-arry "resources/userdata1.parquet"
                  "resources/userdata2.parquet"
                  "resources/userdata3.parquet"
                  "resources/userdata4.parquet"
                  "resources/userdata5.parquet")
    "users"
    (str "select country, count(*) as user_count, cast (avg(salary) as decimal(11,2)) as avg_salary "
         " from users group by country order by country"))))