(ns clstreams.kstreams.helpers
  (:require [clstreams.kstreams :as ks]
            [clstreams.kstreams.component :refer [new-topology]])
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.streams.StreamsConfig))

(def client-props
  (doto (java.util.Properties.)
    (.put "bootstrap.servers" "kafka:9092")
    (.put "acks" "all")
    (.put "retries" (int 0))
    (.put "batch.size" (int 16384))
    (.put "linger.ms" (int 1))
    (.put "buffer.memory" (int 33554432))
    (.put "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
    (.put "value.serializer" "org.apache.kafka.common.serialization.StringSerializer")))

(defn produce-words
  [& lines]
  (let [topic "streams-file-input"
        producer (KafkaProducer. client-props)]
    (doseq [line lines]
      (.send producer (ProducerRecord. topic "" line)))
    (.close producer)))


(def default-pipeline-props
  {StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka:9092"
   StreamsConfig/KEY_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
   StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG 0})

(def print-word-counts-props
  (assoc default-pipeline-props StreamsConfig/APPLICATION_ID_CONFIG "streams-printwordcounts"))

(defn build-print-word-counts []
  (let [builder (KStreamBuilder.)]
    (-> builder
        (ks/stream (Serdes/String) (Serdes/Long) ["streams-wordcount-output"])
        (ks/foreach println))
    builder))

(defn print-word-counts []
  (new-topology print-word-counts-props (build-print-word-counts)))
