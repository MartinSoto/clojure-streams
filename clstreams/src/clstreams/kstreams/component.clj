(ns clstreams.kstreams.component
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.streams KafkaStreams StreamsConfig]))

(defn config->props [config]
  (let [props (java.util.Properties.)]
    (doseq [[key value] config]
      (.put props key value))
    props))

(defrecord Producer [config topic-name producer]
  component/Lifecycle

  (start [component]
    (assoc component
           :config config
           :topic-name topic-name
           :producer (KafkaProducer. (config->props config))))

  (stop [component]
    (.close producer)
    (assoc component :producer nil)))

(defn new-producer [topic-name config]
  (map->Producer {:config config :topic-name topic-name}))

(defn producer-send! [{:keys [producer topic-name]} & msgs]
  (doseq [[key value] msgs]
    (.send producer (ProducerRecord. topic-name key value))))


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
