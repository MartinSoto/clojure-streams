(ns clstreams.webapi
  (:require [clstreams.webapi.component :refer [new-aleph]]
            [ring.middleware.json :refer [wrap-json-body wrap-json-response]]
            [ring.util.response :refer [response]]
            [clstreams.kstreams :as ks])
  (:import org.apache.kafka.streams.state.QueryableStoreTypes))

(defn main-handler [{{kstreams :kstreams} :pipeline} request]
  (let [store (.store kstreams "Counts" (QueryableStoreTypes/keyValueStore))]
    (response (str (.get store "kafka")))))

(defn make-app [component]
  (-> (partial main-handler component)
      wrap-json-body
      wrap-json-response))


(defn web-test []
  (new-aleph make-app {:host "0.0.0.0" :port 8080}))
