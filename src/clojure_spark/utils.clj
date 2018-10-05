(ns clojure-spark.utils
  (:require [flambo.api :as api]
            [flambo.conf :as conf]
            [flambo.sql :as sql])

  (:import [org.apache.spark.sql Column])
  (:gen-class))

(defn build-spark-context[app-name]
  (defonce spark-context (api/spark-context (conf/spark-conf)))
  (defonce sql-context
    (sql/sql-context spark-context)))

(defn build-spark-local-context [app-name]
  (defonce spark-context (api/spark-context "local[*]" app-name))
  (defonce sql-context
    (sql/sql-context spark-context)))


(defn build-columns
  "prepare a column array"
  [& mycols]
  (into-array Column (map (fn [x] (Column. x)) mycols)))

(defn str-arry
  "prepare a string array"
  [& mycols]
  (into-array mycols))

(build-spark-local-context "new-name")

(defn save-file-with-partition[df partition-columns file-name]
  (->
   df
   (.write)
   (.mode SaveMode/Append)
   (.partitionBy (util/str-arry partition-columns))
   (.save file-name)))