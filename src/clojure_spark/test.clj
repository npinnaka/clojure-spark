(def vect-of-maps
  [{:age 27 :first_name "Prakash" :salary 100.03M :fin_rec {:annual_sal 12000.39 :tax "yes"}}
   {:age 32 :first_name "Siva" :salary 101.03M :fin_rec {:annual_sal 12000.39 :tax "yes"}}
   {:age 40 :first_name "Venkat" :salary 105.03M :fin_rec {:annual_sal 12000.39 :tax "yes"}}])

(def vect-of-maps1
  [{:age nil :first_name nil :salary 100.03M}
   {:age nil :first_name "Siva" :salary 101.03M}
   {:age 40 :first_name "Venkat" :salary 105.03M}])

(def vect-of-maps2
  [{:age nil :first_name nil :salary 100.03M}
   {:age nil :first_name "Siva" :salary 101.03M}
   {:age nil :first_name "Venkat" :salary 105.03M}])

(def type-map {java.lang.Long DataTypes/LongType
               java.lang.String DataTypes/StringType
               java.math.BigDecimal (DataTypes/createDecimalType 11 2)
               java.lang.Double DataTypes/DoubleType})

(defn create-stringtype-map-by-default
  [map-val]
  (reduce (fn create-string-type
            [final-map key-value]
            (conj final-map [key-value DataTypes/StringType]))
          {} (keys map-val)))


(defn create-structure
  [vect-of-maps]
  (DataTypes/createStructType (map (fn map-field
                                     [[key-name value]]
                                     (DataTypes/createStructField (name key-name)  value true))
                                     (let [column-count (count (first vect-of-maps)) ]
                                       ;(println (first vect-of-maps))
                                       (merge (create-stringtype-map-by-default (first vect-of-maps))
                                              (loop [final-map {}
                                                     map-val1 vect-of-maps]
                                                (if (or (= (count final-map) column-count) (empty? map-val1))
                                                  final-map
                                                  (do (let [map-val (first map-val1)]
                                                        (recur (reduce (fn [final-map map-key]
                                                                         (when (not (nil? (get map-val map-key)))
                                                                               ;(println "Inside when")
                                                                               ;(println map-val)
                                                                               (if (map? (get map-val map-key))
                                                                                 (merge final-map {map-key (create-structure [(get map-val map-key)])})
                                                                                 (merge final-map {map-key (get type-map (type (get map-val map-key)))})))
                                                                         ) final-map (keys map-val)) (rest map-val1)))))))))))

(defn map->row
  [ map-val]
   ;(println map-val)
  (RowFactory/create (into-array Object (reduce (fn maps [final-vect [map-key map-value]]
                                              (if (map? map-value)
                                                (conj final-vect (map->row map-value))
                                                (conj final-vect map-value))) [] map-val))))

(defn vect->row
  [vect-of-maps]
  (mapv map->row vect-of-maps))


(defn vect-of-maps->parquet
  [vect-of-maps]
  (println (type (get (first vect-of-maps) :fin_rec)))
  (System/setProperty "hadoop.home.dir" "C:\\Users\\DKSE\\")
  (let [ sql-ctx (sql/sql-context sc)
         rdd (.parallelize sc  (vect->row vect-of-maps))
         structure  (create-structure vect-of-maps)
         _ (println structure)
         df (.createDataFrame sql-ctx rdd structure)
  ]
    (println (.show df))
    (.parquet (.write (.coalesce df 1)) "resources/output0/op.parquet")
    ))
