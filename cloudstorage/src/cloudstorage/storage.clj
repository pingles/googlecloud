(ns cloudstorage.storage
  (:require [clojure.java.io :as io])
  (:import [com.google.api.services.storage.model Bucket StorageObject]
           [com.google.api.client.http InputStreamContent]))

(defprotocol ToClojure
  (to-clojure [_]))

(extend-protocol ToClojure
  Bucket
  (to-clojure [bucket] {:id       (.getId bucket)
                        :name     (.getName bucket)
                        :location (.getLocation bucket)
                        :created  (.getTimeCreated bucket)}))

(defn lazy-paginate-seq
  "Builds a lazy sequence of results from fetch-op- a function with two
  arities to handle wfirst result without a pagination token, and second
  with the next page token. result-fn is used to retrieve items of the
  operation result."
  [fetch-op result-fn]
  (loop [items nil
         op    (fetch-op)]
    (let [result    (.execute op)
          token     (.getNextPageToken result)
          new-items (.getItems result)]
      (if (nil? token)
        (lazy-cat items new-items)
        (recur (lazy-cat items new-items) (fetch-op token))))))

(defn list-buckets
  [service project-id]
  (letfn [(mk-list-op
            ([] (-> service (.buckets) (.list project-id)))
            ([token] (doto (-> service (.buckets) (.list project-id))
                       (.setPageToken token))))]
    (->> (lazy-paginate-seq mk-list-op #(.getItems %)) (map to-clojure))))


(extend-protocol ToClojure
  StorageObject
  (to-clojure [obj] {:id           (.getId obj)
                     :md5          (.getMd5Hash obj)
                     :media        (.getMediaLink obj)
                     :size         (.getSize obj)
                     :name         (.getName obj)
                     :updated      (.getUpdated obj)
                     :meta         (.getMetadata obj)
                     :content-type (.getContentType obj)}))

(defn list-objects
  [service bucket]
  (letfn [(mk-list-op
            ([] (-> service (.objects) (.list bucket)))
            ([token] (doto (-> service (.objects) (.list bucket))
                       (.setPageToken token))))]
    (->> (lazy-paginate-seq mk-list-op #(.getItems %)) (map to-clojure))))

(defn get-object
  "Returns an object's metadata. To download the content use the
  function in the :download-to key, this takes an argument that will be
  coerced to an output stream using clojure.java.io/output-stream."
  [service bucket object]
  (let [op (-> service (.objects) (.get bucket object))]
    (when-let [obj (to-clojure (.execute op))]
      (assoc obj :download-to (fn download-to [out & {:keys [resumable]
                                                     :or   {resumable true}}]
                                (-> op (.getMediaHttpDownloader) (.setDirectDownloadEnabled (not resumable)))
                                (.executeMediaAndDownloadTo op (io/output-stream out)))))))


(defn insert-object
  "Inserts an object of name into the specified bucket, media-content is
  coerced into an input stream with clojure.java.io/input-stream."
  [service bucket name media-content & {:keys [content-type]}]
  (let [input (InputStreamContent. content-type
                                   (io/input-stream media-content))
        storage-object (doto (StorageObject. )
                         (.setName name))
        op (-> service (.objects) (.insert bucket storage-object input))]
    (to-clojure (.execute op))))
