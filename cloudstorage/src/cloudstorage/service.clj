(ns cloudstorage.service
  (:import [com.google.api.services.storage StorageScopes Storage$Builder]
           [com.google.api.client.googleapis.javanet GoogleNetHttpTransport]
           [com.google.api.client.json.jackson2 JacksonFactory]))

;; [StorageScopes/DEVSTORAGE_READ_WRITE]

(def scopes {:read-write StorageScopes/DEVSTORAGE_READ_WRITE
             :read       StorageScopes/DEVSTORAGE_READ_ONLY
             :full       StorageScopes/DEVSTORAGE_FULL_CONTROL})

(defn service
  "Creates the CloudStorage service, requires credentials with Cloud
  Storage scope:

  e.g. (googlecloud.credentials/service-credentials ... [(cloudstorage.service/scopes :full)])"
  [credentials & {:keys [application]
                  :or   {application "https://github.com/pingles/googlecloud"}}]
  (let [transport    (GoogleNetHttpTransport/newTrustedTransport)
        json-factory (JacksonFactory. )]
    (.build (doto (Storage$Builder. transport json-factory credentials)
              (.setApplicationName application)))))
