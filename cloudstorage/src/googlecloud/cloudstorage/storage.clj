(ns googlecloud.cloudstorage.storage
  (:require [clojure.java.io :as io]
            [googlecloud.core :as gc])
  (:import [com.google.api.services.storage.model Bucket StorageObject]
           [com.google.api.client.http InputStreamContent]))

(extend-protocol gc/ToClojure
  Bucket
  (to-clojure [bucket] {:id       (.getId bucket)
                        :name     (.getName bucket)
                        :location (.getLocation bucket)
                        :created  (.getTimeCreated bucket)}))

(defn list-buckets
  [service project-id]
  (letfn [(mk-list-op
            ([] (-> service (.buckets) (.list project-id)))
            ([token] (doto (-> service (.buckets) (.list project-id))
                       (.setPageToken token))))]
    (->> (gc/lazy-paginate-seq mk-list-op #(.getItems %)) (map gc/to-clojure))))


(extend-protocol gc/ToClojure
  StorageObject
  (to-clojure [obj] {:id               (.getId obj)
                     :md5              (.getMd5Hash obj)
                     :media            (.getMediaLink obj)
                     :size             (.getSize obj)
                     :name             (.getName obj)
                     :updated          (.getUpdated obj)
                     :meta             (.getMetadata obj)
                     :content-type     (.getContentType obj)
                     :content-encoding (.getContentEncoding obj)}))

(defn list-objects
  [service bucket]
  (letfn [(mk-list-op
            ([] (-> service (.objects) (.list bucket)))
            ([token] (doto (-> service (.objects) (.list bucket))
                       (.setPageToken token))))]
    (->> (gc/lazy-paginate-seq mk-list-op #(.getItems %)) (map gc/to-clojure))))

(defn get-object
  "Returns an object's metadata. To download the content use the
  function in the :download-to key, this takes an argument that will be
  coerced to an output stream using clojure.java.io/output-stream."
  [service bucket object]
  (let [op (-> service (.objects) (.get bucket object))]
    (when-let [obj (gc/to-clojure (.execute op))]
      (assoc obj :download-to (fn download-to [out & {:keys [resumable]
                                                     :or   {resumable true}}]
                                (-> op (.getMediaHttpDownloader) (.setDirectDownloadEnabled (not resumable)))
                                (.executeMediaAndDownloadTo op (io/output-stream out)))))))


(defn insert-object
  "Inserts an object of name into the specified bucket, media-content is
  coerced into an input stream with clojure.java.io/input-stream."
  [service bucket name media-content & {:keys [content-type content-encoding]}]
  (let [input (InputStreamContent. content-type
                                   (io/input-stream media-content))
        storage-object (doto (StorageObject.)
                         (.setName name)
                         (.setContentEncoding content-encoding))
        op (-> service (.objects) (.insert bucket storage-object input))]
    (gc/to-clojure (.execute op))))