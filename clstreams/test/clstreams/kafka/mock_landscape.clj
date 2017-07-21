(ns clstreams.kafka.mock-landscape
  (:require [clstreams.kafka.component :refer [KafkaLandscape]]
            [com.stuartsierra.component :as component]))

(defn assoc-or-close [map key closeable-value]
  (if-let [prev (get map key)]
    (do
      (if (not (identical? closeable-value prev))
        (.close closeable-value))
      map)
    (assoc map key closeable-value)))

(defrecord MockKafkaLandscape [producers consumers]
  component/Lifecycle

  (start [landscape]
    landscape)

  (stop [landscape]
    landscape)

  KafkaLandscape

  (get-producer! [landscape topic]
    (get (swap! producers assoc-or-close topic
                (org.apache.kafka.clients.producer.MockProducer.))
         topic))

  (get-consumer! [landscape topic]
    (get (swap! consumers assoc-or-close topic
                (org.apache.kafka.clients.consumer.MockConsumer.
                 org.apache.kafka.clients.consumer.OffsetResetStrategy/NONE))
         topic)))

(defn new-mock-kafka-landscape []
  (map->MockKafkaLandscape
   {:producers (atom {})
    :consumers (atom {})}))
