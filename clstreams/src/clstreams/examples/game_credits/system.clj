(ns clstreams.examples.game-credits.system
  (:require [clstreams.examples.game-credits.topology :as topology]
            [clstreams.kstreams.component :refer [producer-send!]]
            [clstreams.kstreams.helpers :as helpers]
            [clstreams.kstreams.serdes :as serdes]
            [com.stuartsierra.component :as component])
  (:import org.apache.kafka.common.serialization.Serdes))

(defn game-credits-system []
  (component/system-map
   :producer (helpers/new-manual-producer "game-credits-requests"
                                          {:key-serde (Serdes/String)
                                           :value-serde (serdes/edn-serde)})
   :pipeline (topology/game-credits-topology)
   :states-printer (helpers/new-print-topic "game-credits-state-states-changelog"
                                            {:key-serde (Serdes/String)
                                             :value-serde (serdes/edn-serde)
                                             :color [:black :bg-blue]})
   :errors-printer (helpers/new-print-topic "game-credits-requests-errors"
                                            {:key-serde (Serdes/String)
                                             :value-serde (serdes/edn-serde)
                                             :color [:white :bg-red]})))

(defn send-request [system obj-key request-msg]
  (producer-send! (:producer system) [obj-key request-msg]))
