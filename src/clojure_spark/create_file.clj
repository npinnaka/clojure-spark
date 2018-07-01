(ns clojure-spark.create-file
  (:require [flambo.api :as api]
            [flambo.conf :as conf]
            [flambo.sql :as sql]
            [clojure-spark.utils :as util])
  (:import [org.apache.spark.sql.types DataTypes]
           [org.apache.spark.sql RowFactory])
  (:gen-class))

(def scehma-vec
  [["product_name" DataTypes/StringType true]
   ["manufacturer" DataTypes/StringType true]
   ["quantity" DataTypes/LongType true]
   ["price" (DataTypes/createDecimalType 11 2) true]])

(def schema
  (->
   (map #(DataTypes/createStructField (first %) (second %) (nth % 2)) scehma-vec)
   DataTypes/createStructType))

;;WIP use RowFactory to create a dataframe...
(defn
  prepare-parquet-file-using-custom-schema
  "preapre a parquet file"
  []
  (let [data       [["iPhone" "Apple inc" 255 999.99M]
                                     ["Note 8" "Samsung Electronics" 155 899.99M]
                                     ["Oneplus 6" "Oneplus llc" 55 579.99M]]
        spark-rows (map #(RowFactory/create %) data)
        data-frame (->
                    util/sql-context
                    (.createDataFrame spark-rows schema))]
    (.show data-frame)))