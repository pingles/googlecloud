(ns googlecloud.bigquery.tables-test
  (:require [googlecloud.bigquery.tables :refer :all]
            [clojure.test :refer [deftest testing is are]]))

(def unpartitioned-table
  {:table-reference {:table-id   "table-id" 
                     :project-id "project-id"
                     :dataset-id "dataset-id"}
   :description     "Contains import events"
   :schema          [{:name "event"       :type :string}
                     {:name "timestamp"       :type :timestamp :mode :nullable}]})

(def ingestion-partitioned-table (assoc unpartitioned-table :time-partitioning {:type "DAY"}))

(def field-partitioned-table (assoc unpartitioned-table :time-partitioning {:type "DAY" :field "timestamp"}))

(deftest test-unpartitioned-table
  (let [bq-table (#'googlecloud.bigquery.tables/mk-table unpartitioned-table)
        time-partitioning (. bq-table getTimePartitioning)]
    (is (= nil time-partitioning))))

(deftest test-ingestion-partitioned-table
  (let [bq-table (#'googlecloud.bigquery.tables/mk-table ingestion-partitioned-table)
        time-partitioning (. bq-table getTimePartitioning)]
    (is (= "DAY" (. time-partitioning getType)))
    (is (= nil (. time-partitioning getField)))))

(deftest test-field-partitioned-table
  (let [bq-table (#'googlecloud.bigquery.tables/mk-table field-partitioned-table)
        time-partitioning (. bq-table getTimePartitioning)]
    (is (= "DAY" (. time-partitioning getType)))
    (is (= "timestamp" (. time-partitioning getField)))))

