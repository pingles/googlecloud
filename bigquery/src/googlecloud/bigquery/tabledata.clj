(ns googlecloud.bigquery.tabledata
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Tabledata]
           [com.google.api.services.bigquery.model Table TableDataInsertAllRequest TableDataInsertAllRequest$Rows TableRow])
  (:require [schema.core :as s]))

(defmulti mk-table-row (fn [row]
                         (cond (map? row)  :record
                               (coll? row) :repeated
                               :else       :value)))
(defmethod mk-table-row :record [m]
  (let [table-row (TableRow. )]
    (doseq [[k v] m]
      (.set table-row k (mk-table-row v)))
    table-row))
(defmethod mk-table-row :repeated [coll]
  (map mk-table-row coll))
(defmethod mk-table-row :value [x]
  x)

(defn mk-insert-request-row
  "Builds the insert request row for the input record map m. Optionally takes an insert-id function that returns a deduping identity for the row- retained by bigquery for a few minutes to detect duplicate insertion requests."
  [m & {:keys [insert-id]}]
  (let [row (TableDataInsertAllRequest$Rows. )]
    (.setJson row (mk-table-row m))
    (when insert-id
      (.setInsertId row (insert-id m)))
    row))

(defn insert-all
  "Inserts records in rows, building inserts with mk-insert-request-row. Can use insert-id function to help BigQuery deduplicate insert requests."
  [^Bigquery service project-id dataset-id table-id rows & {:keys [insert-id template-suffix]}]
  (let [data    (map (fn [row]
                       (mk-insert-request-row row :insert-id insert-id))
                     rows)
        request (doto (TableDataInsertAllRequest. )
                  (.setRows data)
                  (.setTemplateSuffix template-suffix))
        op      (-> service (.tabledata) (.insertAll project-id dataset-id table-id request))]
    (.execute op)))
