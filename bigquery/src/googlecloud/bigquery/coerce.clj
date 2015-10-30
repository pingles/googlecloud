(ns googlecloud.bigquery.coerce)

(defprotocol ToClojure
  (to-clojure [x]))
