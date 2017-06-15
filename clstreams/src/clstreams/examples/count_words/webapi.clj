(ns clstreams.examples.count-words.webapi
  (:require [bidi.ring :as bidi]
            [clojure.core.async :refer [<! >! chan go timeout]]
            [clstreams.webapi.component :refer [new-aleph]]
            [manifold.stream :as stream]
            [yada.yada :as yada])
  (:import org.apache.kafka.streams.state.QueryableStoreTypes))

(defn make-word-count-handler [{{kstreams :kstreams} :pipeline}]
  (yada/resource
   {:parameters {:path {:word String}}
    :produces "application/json"
    :methods {:get {:response
                    (fn [ctx]
                      (let [word (get-in ctx [:parameters :path :word])
                            store (.store kstreams "Counts"
                                          (QueryableStoreTypes/keyValueStore))
                            content (chan)]
                        (go
                          (<! (timeout 1))
                          (>! content
                              (let [count (->> word (.get store) (#(if % % 0)))]
                                {:word word
                                 :count count})))
                        (-> content stream/->source stream/take!)))}}}))

(defn make-app [component]
  (bidi/make-handler
   ["/" {["word-count/" :word] (make-word-count-handler component)
         "hello" (yada/handler "Hello World!\n")}]))

(defn web-test []
  (new-aleph make-app {:host "0.0.0.0" :port 8080}))
