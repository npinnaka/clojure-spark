(ns clojure-spark.create-file
    (:require [flambo.api :as api]
              [flambo.conf :as conf]
              [flambo.sql :as sql]
              [clojure-spark.utils :as util])
    (:import [org.apache.spark.sql.types DataTypes]
             [org.apache.spark.sql RowFactory
              SaveMode])
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

(defn prepare-json-file-using-custom-schema
  "preapre a JSON file"
  []
  (let [in-rdd          (api/parallelize util/spark-context
                                         [["iPhone" "Apple inc" 255 999.99M]
                                          ["Note 8" "Samsung Electronics" 155 899.99M]
                                          ["Oneplus 6" "Oneplus llc" 55 579.99M]])

        out-rdd         (->
                         in-rdd
                         (api/map
                          (api/fn [row-vec]
                            (RowFactory/create (into-array Object row-vec)))))

        data-frame      (.createDataFrame util/sql-context
                                          out-rdd
                                          schema)]
    (->
     data-frame
     (.coalesce 1) ; write 1 partition
     (.write) ; returns data -frame
     (.mode SaveMode/Append) ;; APPEND TO FILE
     (.json "resources/jsonfile"))))


(defn prepare-parquet-file-using-custom-schema
  "preapre a parquet file"
  []
  (let [in-rdd          (api/parallelize util/spark-context
                                         [["iPhone" "Apple inc" 255 999.99M]
                                          ["Note 8" "Samsung Electronics" 155 899.99M]
                                          ["Oneplus 6" "Oneplus llc" 55 579.99M]])

        out-rdd         (->
                         in-rdd
                         (api/map
                          (api/fn [row-vec]
                            (RowFactory/create (into-array Object row-vec)))))

        data-frame      (.createDataFrame util/sql-context
                                          out-rdd
                                          schema)]
    (->
     data-frame
     (.coalesce 1) ; write 1 partition
     (.write) ; returns data -frame
     (.mode SaveMode/Append) ;; APPEND TO FILE
     (.parquet "resources/parquetfile"))))