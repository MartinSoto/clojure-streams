(ns clstreams.kafka.mock-landscape
  (:require [clstreams.kafka.component :refer [KafkaLandscape]]
            [com.stuartsierra.component :as component]))

(defrecord MockKafkaLandscape []
  component/Lifecycle

  (start [component])

  (stop [component])

  KafkaLandscape

  (get-producer! [landscape topic]
    (org.apache.kafka.clients.producer.MockProducer.))

  (get-consumer! [landscape topic]
    (org.apache.kafka.clients.consumer.MockConsumer.
     org.apache.kafka.clients.consumer.OffsetResetStrategy/NONE)))

(defn new-mock-kafka-landscape []
  (map->MockKafkaLandscape {}))
