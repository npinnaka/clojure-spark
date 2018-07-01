(ns clojure-spark.query-to-map
  (:require [flambo.api :as api]
            [flambo.sql :as sql]
            [clojure-spark.utils :as util])
  (:gen-class))

(defn create-map
  "create a map using zipmap"
  [columns values]
  (zipmap (map keyword columns) values))

(defn data-frame->map
  "convert data frame to a clojure map"
  [data-frame]
  (let [columns (sql/columns data-frame)
        data
        (->
         data-frame
         (.toJavaRDD)
         (api/map sql/row->vec)
         api/collect)]
    (map #(create-map columns %) data)))

(defn parquet-file-apply-query->data-frame
  "return a dataframe from parquet files using flambo sql"
  [file-name table-name query]
  (let [data-frame    (sql/parquet-file util/sql-context file-name)
        _             (sql/register-temp-table data-frame table-name)
        out-df        (sql/sql util/sql-context query)]
    out-df))

(defn json-file-apply-query->data-frame
  "process a json file using flambo sql"
  [file-name table-name query]
  (let [data-frame    (sql/json-file util/sql-context file-name)
        _             (sql/register-temp-table data-frame table-name)
        out-df        (sql/sql util/sql-context query)]
    out-df))

(defn process-input-parquet-file
  "process a parquet files using flambo sql"
  [file-name table-name query]
  (let [data-frame    (sql/parquet-file util/sql-context file-name)
        _             (sql/register-temp-table data-frame table-name)
        out-df        (sql/sql util/sql-context query)]
    (data-frame->map out-df)))

(defn process-input-json-file
  "process a json file using flambo sql"
  [file-name table-name query]
  (let [data-frame    (sql/json-file util/sql-context file-name)
        _             (sql/register-temp-table data-frame table-name)
        out-df        (sql/sql util/sql-context query)]
    (data-frame->map out-df)))

(defn process-input-json-file-v2
  "process a json file using flambo sql"
  []
  (let [data-frame    (sql/json-file util/sql-context "resources/input.json")

        _             (sql/register-temp-table data-frame "suppliers")

        out-df        (sql/sql util/sql-context
                               (str "select supplier, product_name , avg(quantity) as avg_qty ,"
                                    " avg(unit_cost) as avg_cost from suppliers "
                                    " group by supplier, product_name "))]
    (data-frame->map out-df)))


(defn process-input-json-file-v1
  "process an json file using aprk api"
  []
  (let [data-frame    (sql/json-file util/sql-context "resources/input.json")

        ;; returns map from spark this can used inplace of spark sql.
        out-df        (->
                       data-frame
                       (.groupBy (util/build-columns "supplier" "product_name"))
                       (.avg (util/str-arry "quantity" "unit_cost"))
                       (.withColumnRenamed "avg(quantity)" "avg_qty")
                       (.withColumnRenamed "avg(unit_cost)" "avg_cost"))]
    (data-frame->map out-df)))
