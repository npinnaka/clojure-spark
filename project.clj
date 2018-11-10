(defproject clojure-spark "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.clojure/data.json "0.2.6"]
                 [yieldbot/flambo "0.7.2"]
                 [org.apache.spark/spark-core_2.11 "1.6.3"]
                 [org.apache.spark/spark-streaming_2.11 "1.6.3"]
                 [org.apache.spark/spark-sql_2.11 "1.6.3"]
                 [org.apache.spark/spark-hive_2.11 "1.6.3"]
                 [com.databricks/spark-csv_2.11 "1.5.0"]
                 [com.crealytics/spark-excel_2.11 "0.10.1"]
                 ]
  :aot :all
  :main clojure-spark.core
  :profiles {:provided
             {:dependencies
              [[org.apache.spark/spark-core_2.11 "1.6.3"]]}})