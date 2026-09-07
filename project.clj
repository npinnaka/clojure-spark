(defproject clojure-spark "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/data.json "2.4.0"]
                 [yieldbot/flambo "0.8.2"]
                 [org.apache.spark/spark-core_2.11 "2.4.8"]
                 [org.apache.spark/spark-streaming_2.11 "2.4.8"]
                 [org.apache.spark/spark-sql_2.11 "2.4.8"]
                 [org.apache.spark/spark-hive_2.11 "2.4.8"]
                 [com.databricks/spark-csv_2.11 "1.5.0"]
                 ]
  :aot :all
  :main clojure-spark.core
  :profiles {:provided
             {:dependencies
              [[org.apache.spark/spark-core_2.11 "2.4.8"]]}})