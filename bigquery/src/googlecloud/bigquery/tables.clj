(ns googlecloud.bigquery.tables
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Tables]
           [com.google.api.services.bigquery.model Table TableList$Tables TableReference TableSchema TableFieldSchema])
  (:require [googlecloud.core :as gc]
            [schema.core :as s]
            [clojure.walk :as walk]))

(extend-protocol gc/ToClojure
  TableReference
  (to-clojure [ref] {:project-id (.getProjectId ref)
                     :dataset-id (.getDatasetId ref)
                     :table-id   (.getTableId ref)})
  TableList$Tables
  (to-clojure [tables]
    {:id            (.getId tables)
     :friendly-name (.getFriendlyName tables)
     :reference     (gc/to-clojure (.getTableReference tables))}))

(defn list [^Bigquery service project-id dataset-id]
  (letfn [(mk-list-op
            ([] (-> service (.tables) (.list project-id dataset-id)))
            ([page-token] (doto (mk-list-op)
                            (.setPageToken page-token))))]
    (->> (gc/lazy-paginate-seq mk-list-op #(.getTables %)) (map gc/to-clojure))))

(defn get [service project-id dataset-id table-id]
  (-> service (.tables) (.get project-id dataset-id table-id) (.execute) (gc/to-clojure)))


(def table-reference-schema
  "A reference for a table in a dataset"
  {:table-id s/Str
   :project-id s/Str
   :dataset-id s/Str})

(defn- mk-table-reference [{:keys [table-id project-id dataset-id] :as ref}]
  (doto (TableReference. )
    (.setProjectId project-id)
    (.setDatasetId dataset-id)
    (.setTableId   table-id)))

(def table-field-schema {:name                         s/Str
                         (s/optional-key :description) s/Str
                         :type                         (s/enum :string :integer :float :boolean :timestamp :record)
                         (s/optional-key :mode)        (s/enum :nullable :required :repeated)
                         (s/optional-key :fields)      [(s/recursive #'table-field-schema)]})

(def table-schema
  "BigQuery Table schema"
  {:table-reference              table-reference-schema
   (s/optional-key :description) s/Str
   (s/optional-key :schema)      [table-field-schema]})

(def field-type {:string "STRING"
                 :integer "INTEGER"
                 :float "FLOAT"
                 :boolean "BOOLEAN"
                 :timestamp "TIMESTAMP"
                 :record "RECORD"})

(def field-mode {:nullable "NULLABLE"
                 :required "REQUIRED"
                 :repeated "REPEATED"})

(defn- mk-fields [{:keys [name type mode description fields]
                   :or   {mode :nullable}}]
  (let [s (TableFieldSchema. )]
    (.setName s name)
    (.setDescription s description)
    (.setType s (field-type type))
    (when fields
      (.setFields s (map mk-fields fields)))
    (when mode
      (.setMode s (field-mode mode)))
    s))

(defn- mk-schema [schema]
  (doto (TableSchema.)
    (.setFields (map mk-fields schema))))

(defn- mk-table [{:keys [table-reference description schema] :as table}]
  {:pre [(s/validate table-schema table)]}
  (doto (Table. )
    (.setTableReference (mk-table-reference table-reference))
    (.setDescription    description)
    (.setSchema         (mk-schema schema))))

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

(defn dissoc-nils [m]
  (let [f (fn [[k v]]
            (when (not (nil? v))
              [k v]))]
    (walk/postwalk (fn [x] (if (map? x)
                            (into {} (map f x))
                            x))
                   m)))

(extend-protocol gc/ToClojure
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
                                    :schema          (gc/to-clojure (.getSchema table))})))

(defn insert [^Bigquery service {:keys [table-reference] :as table}]
  (let [op (-> service
               (.tables)
               (.insert (:project-id table-reference) (:dataset-id table-reference) (mk-table table)))]
    (gc/to-clojure (.execute op))))

(defn delete [^Bigquery service project-id dataset-id table-id]
  (let [delete-op (-> service (.tables) (.delete project-id dataset-id table-id))]
    (.execute delete-op)))
