(ns clstreams.kstreams.component
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component])
  (:import [org.apache.kafka.streams KafkaStreams StreamsConfig]))

(defrecord Topology [config builder kstreams]
  component/Lifecycle

  (start [component]
    (let [streams (KafkaStreams. builder (StreamsConfig. config))]
      (log/info "Starting topology")
      (.start streams)
      (assoc component :kstreams streams)))

  (stop [component]
    (log/info "Stopping topology")
    (.close kstreams)
    (assoc component :kstreams nil)))

(defn new-topology [config builder]
  (map->Topology {:config config :builder builder}))
