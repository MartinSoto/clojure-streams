(ns clstreams.examples.game-credits.topology
  (:require [clstreams.examples.game-credits.state :as state]
            [clstreams.kstreams :as ks]
            [clstreams.kstreams.component :refer [new-topology]]
            [clstreams.kstreams.helpers :refer [default-topology-config]]
            [clstreams.kstreams.serdes :as serdes])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams.kstream KStreamBuilder]
           [org.apache.kafka.streams.processor Processor ProcessorSupplier]
           org.apache.kafka.streams.state.Stores
           org.apache.kafka.streams.StreamsConfig))

(def game-credits-props
  (assoc default-topology-config StreamsConfig/APPLICATION_ID_CONFIG "game-credits-state"))

(deftype GameCreditsProcessor [^:unsynchronized-mutable context]

  Processor

  (init [this ctx]
    (set! context ctx))

  (process [this obj-key request]
    (let [state-store (.getStateStore context "game-credits-states")
          old-state (.get state-store obj-key)
          new-state (state/update-credits old-state request)]
      (.put state-store obj-key new-state)))

  (punctuate [this timestamp] nil)

  (close [this] nil))

(deftype GameCreditsProcessorSuppl []

  ProcessorSupplier

  (get [this] (->GameCreditsProcessor nil)))

(defn game-credit-builder []
  (let [builder (KStreamBuilder.)
        states-store-name "game-credits-states"
        states-store (-> (Stores/create states-store-name)
                         (.withKeys (Serdes/String))
                         (.withValues (serdes/edn-serde))
                         .persistent
                         .build)]
    (-> builder
        (.addStateStore states-store (into-array String []))
        (ks/stream (Serdes/String) (serdes/edn-serde) ["game-credits-requests"])
        (ks/process (->GameCreditsProcessorSuppl) [states-store-name]))
    builder))

(defn game-credits-topology []
  (new-topology game-credits-props (game-credit-builder)))

