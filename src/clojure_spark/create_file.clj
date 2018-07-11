(ns clojure-spark.create-file
    (:require [flambo.api :as api]
              [flambo.conf :as conf]
              [flambo.sql :as sql]
              [clojure-spark.utils :as util])
    (:import [org.apache.spark.sql.types DataTypes]
             [org.apache.spark.sql RowFactory
                                   SaveMode])
    (:gen-class))

(def types-map
  {java.lang.Boolean    DataTypes/BooleanType
   java.lang.Integer    DataTypes/IntegerType
   java.lang.Long       DataTypes/LongType
   clojure.lang.BigInt  DataTypes/LongType
   java.lang.String     DataTypes/StringType
   java.lang.Double     DataTypes/DoubleType
   java.math.BigDecimal (DataTypes/createDecimalType 11 2)
   }) ;; nil tyopes is not need for non matching type we will see nil's only

(defn identify-valid-map-to-create-structure [vec-map]  ;; this function will fail if all maps has at least one nil value.
  (let [first-map (first vec-map)]
    (if (nil? first-map)
      first-map
      (if (.contains (map type (vals first-map)) nil)
        (identify-valid-map-to-create-structure (rest vec-map))
        first-map))))

;;WIP write an optimal method for identifying data types.
(defn identity-data-types [vec-map]
    ;; use range for keys
  ;; calls  (map type (vals first-map) and zpply zipmap
  ;; update only nil record by iterating rest of vec map
  ;; if all types identified then return from function.
    )

(defn create-structure
  [vec-map]
  (DataTypes/createStructType
   (map
    (fn map-field
      [[k v]]
      (DataTypes/createStructField (name k) (get types-map (type v)) true))
    vec-map)))

(defn generate-parquet-file
  [vec-map]
  (let [sql-ctx (sql/sql-context util/spark-context)

        rdd     (->
                 (api/parallelize util/spark-context (->>
                                                      vec-map
                                                      (map vals)
                                                      (map vec)
                                                      (vec)))
                 (api/map
                  (api/fn [vec-row]
                    (RowFactory/create (into-array Object vec-row)))))

        df      (.createDataFrame sql-ctx rdd (create-structure
                                               (identify-valid-map-to-create-structure vec-map)))]
    (.show df)
    (->
     df
     (.coalesce 1)
     (.write)
     (.mode SaveMode/Overwrite)
     (.parquet "resources/output/op.parquet"))))
