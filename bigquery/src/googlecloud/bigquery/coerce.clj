(ns googlecloud.bigquery.coerce
  (:require [googlecloud.core :as gc]
            [clojure.walk :as walk])
  (:import [com.google.api.services.bigquery.model JobList Job JobConfigurationLoad JobConfigurationExtract JobConfigurationQuery JobConfiguration JobStatus JobStatistics JobList$Jobs JobReference GetQueryResultsResponse TableRow TableCell TableSchema TableFieldSchema TableReference]
           [com.google.api.services.bigquery Bigquery Bigquery$Tables]
           [com.google.api.services.bigquery.model Table TableList$Tables TableReference TableSchema TableFieldSchema]
           [java.util Date]))

(defn dissoc-nils [m]
  (let [f (fn [[k v]]
            (when (not (nil? v))
              [k v]))]
    (walk/postwalk (fn [x] (if (map? x)
                            (into {} (map f x))
                            x))
                   m)))

(extend-protocol gc/ToClojure
  JobReference
  (to-clojure [ref] {:project-id (.getProjectId ref)
                     :job-id     (.getJobId ref)})

  JobList$Jobs
  (to-clojure [job] {:status        (gc/to-clojure (.getStatus job))
                     :job-reference (gc/to-clojure (.getJobReference job))
                     :statistics    (gc/to-clojure (.getStatistics job))})

  JobStatus
  (to-clojure [status] {:state  (.getState status)
                        :errors (.getErrors status)})
  JobStatistics
  (to-clojure [statistics] {:started (when-let [e (.getStartTime statistics)]
                                       (Date. e))
                            :ended   (when-let [e (.getEndTime   statistics)]
                                       (Date. e))})
  Job
  (to-clojure [job] {:job-reference (gc/to-clojure (.getJobReference job))
                     :status        (gc/to-clojure (.getStatus job))
                     :statistics    (gc/to-clojure (.getStatistics job))})
  JobList
  (to-clojure [list]
    (map gc/to-clojure (.getJobs list)))

  TableCell
  (to-clojure [cell] (.getV cell))

  TableRow
  (to-clojure [row] (map gc/to-clojure (.getF row))))


(defn mode->clojure [s]
  ({"NULLABLE" :nullable
    "REQUIRED" :required
    "REPEATED" :repeated} s))

(defn type->clojure [t]
  ({"STRING"    :string
    "INTEGER"   :integer
    "FLOAT"     :float
    "BOOLEAN"   :boolean
    "TIMESTAMP" :timestamp
    "RECORD"    :record} t))

(extend-protocol gc/ToClojure
  TableReference
  (to-clojure [ref] {:project-id (.getProjectId ref)
                     :dataset-id (.getDatasetId ref)
                     :table-id   (.getTableId ref)})
  TableList$Tables
  (to-clojure [tables]
    {:id              (.getId tables)
     :friendly-name   (.getFriendlyName tables)
     :table-reference (gc/to-clojure (.getTableReference tables))})

  TableFieldSchema
  (to-clojure [field-schema] {:name        (.getName field-schema)
                              :description (.getDescription field-schema)
                              :mode        (mode->clojure (.getMode field-schema))
                              :type        (type->clojure (.getType field-schema))
                              :fields      (seq (map gc/to-clojure (.getFields field-schema)))})
  TableSchema
  (to-clojure [schema] (map gc/to-clojure (.getFields schema)))
  Table
  (to-clojure [table] (dissoc-nils {:id              (.getId table)
                                    :table-reference (gc/to-clojure (.getTableReference table))
                                    :friendly-name   (.getFriendlyName table)
                                    :description     (.getDescription table)
                                    :schema          (when-let [s (.getSchema table)]
                                                       (gc/to-clojure s))})))
