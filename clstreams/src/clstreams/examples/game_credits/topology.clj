(ns clstreams.examples.game-credits.topology
  (:require [clstreams.examples.game-credits.state :as state]
            [clstreams.kafka.component :refer [new-topology]]
            [clstreams.kstreams :as ks]
            [clstreams.kstreams.helpers :refer [default-topology-config]]
            [clstreams.kstreams.serdes :as serdes])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder Transformer TransformerSupplier]
           org.apache.kafka.streams.state.Stores))

(def game-credits-props
  (assoc default-topology-config StreamsConfig/APPLICATION_ID_CONFIG "game-credits-state"))

(def states-store-name "states")

(deftype GameCreditsProcessor [^:unsynchronized-mutable context]

  Transformer

  (init [this ctx]
    (set! context ctx))

  (transform [this obj-key request]
    (let [state-store (.getStateStore context states-store-name)
          old-state (.get state-store obj-key)
          new-state (state/update-credits old-state request)]
      (if (contains? new-state :errors)
        (KeyValue. obj-key new-state)
        (do
          (.put state-store obj-key new-state)
          nil))))

  (punctuate [this timestamp] nil)

  (close [this] nil))

(deftype GameCreditsProcessorSuppl []

  TransformerSupplier

  (get [this] (->GameCreditsProcessor nil)))

(comment :update-me defn game-credit-builder []
  (let [builder (KStreamBuilder.)
        states-store (-> (Stores/create states-store-name)
                         (.withKeys (Serdes/String))
                         (.withValues (serdes/edn-serde))
                         .persistent
                         (.enableLogging {})
                         .build)]
    (-> builder
        (.addStateStore states-store (into-array String []))
        (ks/stream (Serdes/String) (serdes/edn-serde) ["game-credits-requests"])
        (ks/transform (->GameCreditsProcessorSuppl) [states-store-name])
        (ks/to (Serdes/String) (serdes/edn-serde) "game-credits-requests-errors"))
    builder))

(comment :update-me defn game-credits-topology []
  (new-topology game-credits-props (game-credit-builder)))

