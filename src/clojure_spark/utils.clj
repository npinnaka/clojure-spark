(ns clojure-spark.utils
  (:require [flambo.api :as api]
            [flambo.conf :as conf]
            [flambo.sql :as sql])
  (:import [org.apache.spark.sql.types DataTypes]
           [org.apache.spark.sql Column
            RowFactory
            SaveMode])
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
  (into-array String mycols))

(build-spark-local-context "new-name")

(defn save-file-with-partition[df partition-columns file-name]
  (->
   df
   (.write)
   (.mode SaveMode/Append)
   ;(.partitionBy (into-array partition-columns))
   (.save file-name)))

(def schema-vec
  {:Name          DataTypes/StringType
   :Age           DataTypes/LongType
   :Occupation    DataTypes/StringType
   :Date_of_birth DataTypes/StringType})

(defn create-structure
  [vec-map]
  (DataTypes/createStructType
   (map
    (fn map-field
      [[k v]]
      (DataTypes/createStructField (name k) v true))
    vec-map)))

(create-structure schema-vec)

(defn read-excel->df [schema file-name sheet-name]
  (let [options (new java.util.HashMap)]
    (.put options "sheetName" sheet-name)
    (.put options "useHeader" "true")
    (.put options "userSchema" "true")
    (-> sql-context
        (.read)
        (.format "com.crealytics.spark.excel")
        (.options options)
        (.schema schema)
        (.load file-name))))

(create-structure schema-vec)

(def df
  (read-excel->df (create-structure schema-vec) "resources/People.xls" "Info"))

(.printSchema df)
(.show df)


(comment


  import org.apache.spark.sql._
  import org.apache.spark.sql.types._
  import com.crealytics.spark.excel._


  val peopleSchema = StructType
  (Array
   (StructField ("Name", StringType, nullable = false),
                StructField ("Age", LongType, nullable = false),
                StructField ("Occupation", StringType, nullable = false),
                StructField ("Date of birth", StringType, nullable = false)))


  val df = spark.read.format ("com.crealytics.spark.excel") .option ("sheetName", "Info") .option ("useHeader", "true") .option ("userSchema", "true") .schema (peopleSchema) .load ("People.xls"))

