(defproject googlecloud/bigquery "0.3.10"
  :description "BijQuery"
  :url "https://github.com/pingles/googlecloud"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [prismatic/schema "1.0.1"]
                 [com.google.api-client/google-api-client "1.20.0"]
                 [com.google.apis/google-api-services-bigquery "v2-rev297-1.22.0"]
                 [googlecloud/core "0.3.4"]])
