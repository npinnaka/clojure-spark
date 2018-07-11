
(def types-map
  {java.lang.Boolean    DataTypes/BooleanType
   java.lang.Integer    DataTypes/IntegerType
   java.lang.Long       DataTypes/LongType
   clojure.lang.BigInt  DataTypes/LongType
   java.lang.String     DataTypes/StringType
   java.lang.Double     DataTypes/DoubleType
   java.math.BigDecimal (DataTypes/createDecimalType 11 2)
   nil                  nil})

(defn create-structure
  [map-val]
  (DataTypes/createStructType
    (map
      (fn map-field
        [[k v]]
        (DataTypes/createStructField (name k) (get types-map (type v)) true))
      map-val)))

(defn generate-structure [vec-map]
  (let [first-map (first vec-map)]
    (if (nil? first-map)
      first-map
      (if (.contains (map type (vals first-map)) nil)
        (generate-structure (rest vec-map))
        first-map))))

(defn generateParquet
  [map-val]
  (let [sql-ctx (sql/sql-context sc)

        rdd  (->
              (f/parallelize sc (->
                                 map-val
                                 (map vals)
                                 (map vec)
                                 (vec)))
              (f/map
               (f/fn [vec-row]
                 (RowFactory/create (into-array Object vec-row)))))

        df      (.createDataFrame sql-ctx rdd (create-structure  (generate-structure map-val)))]
    (.show df)
    (.parquet (.write (.coalesce df 1)) "resources/output/op.parquet")))


(def map-val [{:age 23 :first_name "mike" :sal 10000.01M :x nil}
              {:age 23 :first_name "mike" :sal 10000.01M :x false}])

(create-structure  map-val)
(create-structure (generate-structure map-val))
(generateParquet map-val)
