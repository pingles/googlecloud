(defproject googlecloud/cloudstorage "0.2.0-SNAPSHOT"
  :description "Google Cloud Storage client"
  :url "https://github.com/pingles/googlecloud"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.google.api-client/google-api-client "1.20.0"]
                 [com.google.apis/google-api-services-storage "v1-rev50-1.20.0"]
                 [googlecloud/core "0.2.0-SNAPSHOT"]])
