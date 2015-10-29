(ns bigquery.tabledata-test
  (:require [bigquery.tabledata :refer :all]
            [clojure.test :refer :all]))

(deftest insert-request-rows
  (let [r (mk-insert-request-row {"name" "paul"})]
    (is (= {"json" {"name" "paul"}} r)))
  (let [r (mk-insert-request-row {"name" "paul"
                                  "addresses" [{"line1" "here"}]})
        d (get r "json")]
    (is (= "paul" (get d "name")))
    (is (= [{"line1" "here"}] (get d "addresses"))))
  (let [r (mk-insert-request-row {"name" "paul"
                                  "id"   "1234"}
                                 :insert-id (fn [m] (get m "id")))]
    (is (= "1234" (.getInsertId r)))))
