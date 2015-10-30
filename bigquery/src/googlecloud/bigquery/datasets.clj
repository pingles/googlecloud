(ns googlecloud.bigquery.datasets
  (:require [googlecloud.core :as gc])
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Datasets]
           [com.google.api.services.bigquery.model Dataset DatasetList$Datasets DatasetReference]))

(extend-protocol gc/ToClojure
  Dataset
  (to-clojure [ds]
    {:id            (.getId ds)
     :friendly-name (.getFriendlyName ds)
     :description   (.getDescription ds)
     :reference     (.getDatasetReference ds)})
  DatasetReference
  (to-clojure [ref] {:dataset-id (.getDatasetId ref)
                     :project-id (.getProjectId ref)})
  DatasetList$Datasets
  (to-clojure [ds] {:id (.getId ds)
                    :friendly-name (.getFriendlyName ds)
                    :reference (gc/to-clojure (.getDatasetReference ds))}))

(defn list [^Bigquery service project-id]
  (let [list-op (-> service (.datasets) (.list project-id))]
    (->> (.execute list-op)
         (.getDatasets)
         (map gc/to-clojure))))

(defn insert [^Bigquery service project-id {:keys [id friendly-name description] :as dataset}]
  {:pre [(not (nil? id))]}
  (let [dataset   (doto (Dataset. )
                    (.setDatasetReference (doto (DatasetReference. )
                                            (.setDatasetId id)))
                    (.setFriendlyName friendly-name)
                    (.setDescription description))
        insert-op (-> service (.datasets) (.insert project-id dataset))]
    (gc/to-clojure (.execute insert-op))))

(defn delete [^Bigquery service project-id dataset-id]
  (let [delete-op (-> service (.datasets) (.delete project-id dataset-id))]
    (.execute delete-op)))
