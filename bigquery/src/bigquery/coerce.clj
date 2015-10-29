(ns bigquery.coerce)

(defprotocol ToClojure
  (to-clojure [x]))
