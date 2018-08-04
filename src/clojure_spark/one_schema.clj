(ns clojure-spark.one-schema
  (:require [flambo.api :as api]
            [flambo.conf :as conf]
            [flambo.sql :as sql]
            [clojure.set :as cset]
            [clojure-spark.utils :as util])
  (:import [org.apache.spark.sql.types DataTypes]
           [org.apache.spark.sql RowFactory
            SaveMode])
  (:gen-class))

(def base-schema
  [[:a DataTypes/StringType]
   [:b DataTypes/LongType]])

(def input-enrichment-schema
  [[:a1 DataTypes/StringType]
   [:b1 DataTypes/LongType]])

(def calculate-enrichment-schema
  [[:a2 DataTypes/StringType]
   [:b2 DataTypes/LongType]])

(def output-enrichment-schema
  [[:a3 DataTypes/StringType]
   [:b3 DataTypes/LongType]])

(def input-template base-schema)

(def input-out-template
  (cset/union base-schema input-enrichment-schema))

(def calculate-template input-out-template)
(def calculate-out-template
  (cset/union input-out-template calculate-enrichment-schema))

(def output-template calculate-out-template)
(def output-out-template
  (cset/union calculate-out-template output-enrichment-schema))

(defn prepare-in-schema [template-name]
  (vec (map #(first %) template-name)))

(prepare-in-schema input-template)
(prepare-in-schema calculate-template)
(prepare-in-schema output-template)

(defn prepare-out-schema [template-name]
  (vec (map #(vector (first %) (second %)) template-name)))

(prepare-out-schema input-out-template)
(prepare-out-schema calculate-out-template)
(prepare-out-schema output-out-template)
