(defproject googlecloud "0.3.5"
  :description "Google Cloud service clients for Clojure"
  :url "https://github.com/pingles/googlecloud"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-sub "0.3.0"]]
  :sub ["core" "bigquery" "cloudstorage"]
  :eval-in-leiningen true)
