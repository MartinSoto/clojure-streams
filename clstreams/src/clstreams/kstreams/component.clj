(ns clstreams.kstreams.component
  (:require [com.stuartsierra.component :as component])
  (:import [org.apache.kafka.streams KafkaStreams StreamsConfig]))

(defrecord Topology [config builder kstreams]
  component/Lifecycle

  (start [component]
    (let [streams (KafkaStreams. builder (StreamsConfig. config))]
      (.start streams)
      (assoc component :kstreams streams)))

  (stop [component]
    (.close kstreams)
    (assoc component :kstreams nil)))

(defn new-topology [config builder]
  (map->Topology {:config config :builder builder}))
