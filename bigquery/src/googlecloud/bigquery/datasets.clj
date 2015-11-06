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
     :location      (.getLocation ds)
     :reference     (gc/to-clojure (.getDatasetReference ds))})
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

(defn get [^Bigquery service project-id dataset-id]
  (let [get-op (-> service (.datasets) (.get project-id dataset-id))]
    (->> (.execute get-op)
         (gc/to-clojure))))

(defn- map->reference [{:keys [dataset-id project-id]}]
  (doto (DatasetReference. )
    (.setDatasetId dataset-id)
    (.setProjectId project-id)))

(defn insert [^Bigquery service project-id {:keys [friendly-name description location reference] :as dataset}]
  (let [dataset   (doto (Dataset. )
                    (.setDatasetReference (map->reference reference))
                    (.setFriendlyName     friendly-name)
                    (.setDescription      description)
                    (.setLocation         (or location "US")))
        insert-op (-> service (.datasets) (.insert project-id dataset))]
    (gc/to-clojure (.execute insert-op))))

(defn delete [^Bigquery service project-id dataset-id]
  (let [delete-op (-> service (.datasets) (.delete project-id dataset-id))]
    (.execute delete-op)))
