(ns googlecloud.bigquery.jobs
  (:require [googlecloud.core :as gc]
            [googlecloud.bigquery.coerce])
  (:import [java.util Date]
           [com.google.api.services.bigquery.model Job TableReference JobConfigurationExtract JobConfiguration JobConfigurationLoad JobConfigurationQuery]))

(defn list [service project-id]
  (let [op (-> service (.jobs) (.list project-id))]
    (gc/to-clojure (.execute op))))

(defn get [service project-id job-id]
  (let [op (-> service (.jobs) (.get project-id job-id))]
    (gc/to-clojure (.execute op))))

(defn insert [service project-id job]
  (let [op (-> service (.jobs) (.insert project-id job))]
    (gc/to-clojure (.execute op))))


(defn- mk-table-reference [{:keys [dataset-id project-id table-id]}]
  (doto (TableReference. )
    (.setDatasetId dataset-id)
    (.setProjectId project-id)
    (.setTableId table-id)))

(defn extract-job
  [{:keys [dataset-id project-id table-id] :as table-reference} destination-uris & {:keys [destination-format]
                                                                                    :or   {destination-format :json}}]
  (let [extract (JobConfigurationExtract.)]
    (.setSourceTable extract (mk-table-reference table-reference))
    (.setDestinationUris extract destination-uris)
    (.setDestinationFormat extract ({:avro "AVRO"
                                     :json "NEWLINE_DELIMITED_JSON"
                                     :csv  "CSV"} destination-format))
    (doto (Job.)
      (.setConfiguration (-> (JobConfiguration.) (.setExtract extract))))))

(defn load-job
  "Creates a job to load data from sources into the table, identified by
  its table reference. "
  [{:keys [dataset-id project-id table-id] :as table-reference} source-uris & {:keys [create-disposition source-format write-disposition quote skip-leading allow-quoted-newlines max-bad-records]
                                                                               :or   {source-format :json
                                                                                      allow-quoted-newlines false
                                                                                      create-disposition :never
                                                                                      write-disposition :append}}]
  (let [load (JobConfigurationLoad.)]
    (.setDestinationTable load (mk-table-reference table-reference))
    (.setSourceUris load source-uris)
    (when quote
      (.setQuote load quote))
    (when skip-leading
      (.setSkipLeadingRows load (int skip-leading)))
    (when max-bad-records
      (.setMaxBadRecords load (int max-bad-records)))
    (.setAllowQuotedNewlines load allow-quoted-newlines)
    (.setSourceFormat load ({:json "NEWLINE_DELIMITED_JSON"
                             :csv  "CSV"} source-format))
    (.setCreateDisposition load ({:never  "CREATE_NEVER"
                                  :needed "CREATE_NEEDED"} create-disposition))
    (.setWriteDisposition load ({:append   "WRITE_APPEND"
                                 :empty    "WRITE_EMPTY"
                                 :truncate "WRITE_TRUNCATE"} write-disposition))
    (doto (Job.)
      (.setConfiguration (-> (JobConfiguration.) (.setLoad load))))))


(def query-priority {:interactive "INTERACTIVE"
                     :batch       "BATCH"})

(defn query-job [query-statement & {:keys [use-cache priority flatten allow-large
                                           write-disposition create-disposition destination-table-reference]
                                    :or   {use-cache          false
                                           priority           :interactive
                                           flatten            true
                                           allow-large        false
                                           write-disposition  :truncate
                                           create-disposition :never}}]
  (let [query (JobConfigurationQuery. )]
    (when destination-table-reference
      (.setDestinationTable query (mk-table-reference destination-table-reference)))
    (.setUseQueryCache     query use-cache)
    (.setPriority          query (query-priority priority))
    (.setFlattenResults    query flatten)
    (.setAllowLargeResults query allow-large)
    (.setQuery             query query-statement)
    (.setCreateDisposition query ({:never  "CREATE_NEVER"
                                   :needed "CREATE_NEEDED"} create-disposition))
    (.setWriteDisposition  query ({:append   "WRITE_APPEND"
                                   :empty    "WRITE_EMPTY"
                                   :truncate "WRITE_TRUNCATE"} write-disposition))
    (doto (Job.)
      (.setConfiguration (-> (JobConfiguration. ) (.setQuery query))))))

(defn query-results [service project-id job-id]
  (letfn [(mk-results-op
            ([] (-> service (.jobs) (.getQueryResults project-id job-id)))
            ([token] (doto (mk-results-op)
                       (.setPageToken token))))]
    (loop [rows   nil
           op     (mk-results-op)]
      (let [result   (.execute op)
            token    (.getPageToken result)
            new-rows (.getRows result)]
        (if (nil? token)
          (let [rows (map gc/to-clojure (concat rows new-rows))]
            {:rows      rows
             :schema    (gc/to-clojure (.getSchema result))
             :bytes     (.getTotalBytesProcessed result)
             :cache-hit (.getCacheHit result)})
          (recur (concat rows new-rows) (mk-results-op token)))))))
