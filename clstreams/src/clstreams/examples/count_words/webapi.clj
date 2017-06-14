(ns clstreams.examples.count-words.webapi
  (:require [bidi.ring :as bidi]
            [clojure.core.async :refer [<! go timeout]]
            [clstreams.webapi.component :refer [new-aleph]]
            [manifold.stream :as stream]
            [ring.middleware.json :refer [wrap-json-body wrap-json-response]]
            [ring.util.response :refer [response]]
            [yada.yada :as yada])
  (:import org.apache.kafka.streams.state.QueryableStoreTypes))

(defn make-word-count-handler-xx [{{kstreams :kstreams} :pipeline}]
  (fn [{:keys [route-params]}]
    (let [store (.store kstreams "Counts" (QueryableStoreTypes/keyValueStore))]
      (->> (:word route-params) (.get store) (#(if % % 0)) str response))
    ))

(defn make-word-count-handler [{{kstreams :kstreams} :pipeline}]
  (yada/resource
   {:parameters {:path {:word String}}
    :methods {:get {:produces {:media-type "text/plain"}
                    :response
                    (fn [ctx]
                      (let [word (get-in ctx [:parameters :path :word])
                            store (.store kstreams "Counts"
                                          (QueryableStoreTypes/keyValueStore))]
                        (->> word (.get store) (#(if % % 0)) str)))}}}))

(defn make-main-handler [component]
  (bidi/make-handler
   ["/" {["word-count/" :word] (make-word-count-handler component)
         "hello" (yada/handler "Hello World!\n")}]))

(defn make-async-handler [component]
  (fn [request]
    (let [handler (make-main-handler component)]
      (-> (go
            (<! (timeout 1))
            (handler request))
          stream/->source
          stream/take!))))

(defn make-app [component]
  (-> (make-main-handler component)
      wrap-json-body
      wrap-json-response))


(defn web-test []
  (new-aleph make-app {:host "0.0.0.0" :port 8080}))
