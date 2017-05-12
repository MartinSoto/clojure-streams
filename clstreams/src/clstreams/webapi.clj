(ns clstreams.webapi
  (:require [bidi.ring :as bidi]
            [clojure.core.async :refer [<! go timeout]]
            [clstreams.webapi.component :refer [new-aleph]]
            [manifold.stream :as stream]
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

(defn make-async-handler [component]
  (fn [request]
    (let [handler (make-main-handler component)]
      (-> (go
            (<! (timeout 900))
            (handler request))
          stream/->source
          stream/take!))))

(defn make-app [component]
  (-> (make-async-handler component)
      wrap-json-body
      wrap-json-response))


(defn web-test []
  (new-aleph make-app {:host "0.0.0.0" :port 8080}))
