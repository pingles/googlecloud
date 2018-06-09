(defproject googlecloud/cloudstorage "0.3.5"
  :description "Google Cloud Storage client"
  :url "https://github.com/pingles/googlecloud"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.google.api-client/google-api-client "1.23.0"]
                 [com.google.apis/google-api-services-storage "v1-rev133-1.23.0"]
                 [googlecloud/core "0.3.5"]])
