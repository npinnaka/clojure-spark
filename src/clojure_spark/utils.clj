(ns clojure-spark.utils
  (:require [flambo.api :as api]
            [flambo.conf :as conf]
            [flambo.sql :as sql])

  (:import [org.apache.spark.sql Column])
  (:gen-class))

(defonce spark-context
  (let [context (-> (conf/spark-conf)
                    (conf/master "local[*]")
                    (conf/app-name "clojure-spark")
                    (api/spark-context))]
    (.setLogLevel context "WARN")
    context))

(defonce sql-context
  (sql/sql-context spark-context))

(defn build-columns
  "prepare a column array"
  [& mycols]
  (into-array Column (map (fn [x] (Column. x)) mycols)))

(defn str-arry
  "prepare a string array"
  [& mycols]
  (into-array mycols))
