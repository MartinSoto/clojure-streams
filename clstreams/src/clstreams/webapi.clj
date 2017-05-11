(ns clstreams.webapi
  (:require [bidi.ring :as bidi]
            [clstreams.webapi.component :refer [new-aleph]]
            [ring.middleware.json :refer [wrap-json-body wrap-json-response]]
            [ring.util.response :refer [response]])
  (:import org.apache.kafka.streams.state.QueryableStoreTypes))

(defn make-word-count-handler [{{kstreams :kstreams} :pipeline}]
  (fn [{:keys [route-params]}]
    (let [store (.store kstreams "Counts" (QueryableStoreTypes/keyValueStore))]
      (->> (:word route-params) (.get store) str response))
    ))

(defn make-main-handler [component]
  (bidi/make-handler
   [["/word-count/" :word] (make-word-count-handler component)]))

(defn make-app [component]
  (-> (make-main-handler component)
      wrap-json-body
      wrap-json-response))


(defn web-test []
  (new-aleph make-app {:host "0.0.0.0" :port 8080}))
