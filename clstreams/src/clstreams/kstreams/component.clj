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

(defrecord Producer [config key-serde value-serde topic-name producer]
  component/Lifecycle

  (start [component]
    (if producer
      component
      (assoc component
             :producer (if (and key-serde value-serde)
                         (KafkaProducer. (config->props config) key-serde value-serde)
                         (KafkaProducer. (config->props config))))))

  (stop [component]
    (if producer
      (do (.close producer)
          (assoc component :producer nil))
      component)))

(defn new-producer
  ([topic-name config]
   (map->Producer {:config config
                   :topic-name topic-name}))
  ([topic-name config key-serde value-serde]
   (map->Producer {:config config
                   :key-serde key-serde
                   :value-serde value-serde
                   :topic-name topic-name})))

(defn producer-send! [{:keys [producer topic-name]} msg & msgs]
  (doseq [[key value] (cons msg msgs)]
    @(.send producer (ProducerRecord. topic-name key value))))


(defrecord Topology [config builder kstreams]
  component/Lifecycle

  (start [component]
    (if kstreams
      component
      (let [streams (KafkaStreams. builder (StreamsConfig. config))]
        (log/info (get config StreamsConfig/APPLICATION_ID_CONFIG) "Starting topology")
        (.start streams)
        (assoc component :kstreams streams))))

  (stop [component]
    (if kstreams
      (do (log/info (get config StreamsConfig/APPLICATION_ID_CONFIG) "Stopping topology")
          (.close kstreams)
          (assoc component :kstreams nil))
      component)))

(defn new-topology [config builder]
  (map->Topology {:config config :builder builder}))
