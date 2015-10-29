(ns googlecloud.credentials
  (:require [clojure.java.io :as io])
  (:import [com.google.api.client.googleapis.auth.oauth2 GoogleCredential$Builder]
           [com.google.api.client.googleapis.javanet GoogleNetHttpTransport]
           [com.google.api.client.json.jackson2 JacksonFactory]))

(defn service-credentials
  "Uses Google Cloud Service Account credentials to authenticate the
  client. Requires a service account ID (usually
  xxxx@developer.gserviceaccount.com), and the p12 private key
  file. Scopes is a sequence of service scopes, e.g. StorageScopes/DEVSTORAGE_READ_WRITE"
  [account-id private-key-file scopes]
  (let [credential (doto (GoogleCredential$Builder. )
                     (.setTransport (GoogleNetHttpTransport/newTrustedTransport))
                     (.setJsonFactory (JacksonFactory. ))
                     (.setServiceAccountId account-id)
                     (.setServiceAccountScopes scopes)
                     (.setServiceAccountPrivateKeyFromP12File (io/file private-key-file)))]
    (.build credential)))
