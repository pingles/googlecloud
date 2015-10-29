(ns bigquery.auth
  (:import [com.google.api.client.googleapis.auth.oauth2 GoogleCredential$Builder]
           [com.google.api.client.googleapis.javanet GoogleNetHttpTransport]
           [com.google.api.client.json.jackson2 JacksonFactory]
           [com.google.api.services.bigquery BigqueryScopes Bigquery$Builder])
  (:require [clojure.java.io :as io]))

(defn- service-credentials
  [account-id private-key-file]
  (let [credential (doto (GoogleCredential$Builder. )
                     (.setTransport (GoogleNetHttpTransport/newTrustedTransport))
                     (.setJsonFactory (JacksonFactory. ))
                     (.setServiceAccountId account-id)
                     (.setServiceAccountScopes [BigqueryScopes/BIGQUERY])
                     (.setServiceAccountPrivateKeyFromP12File (io/file private-key-file)))]
    (.build credential)))

(defn service [service-account-id private-key-file & {:keys [application]
                                                      :or   {:application "BijQuery"}}]
  (let [creds (service-credentials service-account-id private-key-file)
        transport    (GoogleNetHttpTransport/newTrustedTransport)
        json-factory (JacksonFactory. )]
    (.build (doto (Bigquery$Builder. transport json-factory creds)
              (.setApplicationName application)))))
