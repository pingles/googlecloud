(ns googlecloud.core)

(defprotocol ToClojure
  (to-clojure [_]))

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
