(ns clstreams.examples.count-words.system
  (:require [clstreams.examples.count-words.pipelines :as pipelines]
            [clstreams.examples.count-words.webapi :as webapi]
            [clstreams.kafka.component :refer [producer-send!]]
            [clstreams.kstreams.helpers :as helpers]
            [com.stuartsierra.component :as component])
  (:import org.apache.kafka.common.serialization.Serdes))

(defn count-words-system []
  (component/system-map
   :producer (helpers/new-manual-producer "streams-file-input")
   :pipeline (pipelines/count-words)
   :printer (helpers/new-print-topic "streams-wordcount-output"
                                     {:key-serde (Serdes/String)
                                      :value-serde (Serdes/Long)
                                      :color [:black :bg-red]})
   :webapi (component/using
            (webapi/web-test)
            [:pipeline])))

(defn produce-words [system & lines]
  (apply producer-send! (:producer system) (for [line lines] ["" line])))
