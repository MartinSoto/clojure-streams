(ns clstreams.kstreams.helpers
  (:require [clojure.pprint :refer [pprint]]
            [clstreams.kstreams :as ks]
            [clstreams.kstreams.component :refer [new-topology]])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
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

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn pprint-topic-message [topic-name key value]
  (let [formatted-key (with-out-str (pprint key))
        formatted-value (with-out-str (pprint value))]
    (println (format "[%s] %s : %s" topic-name key value))))

(defn new-print-topic
  ([topic-name]
   (new-print-topic topic-name {}))
  ([topic-name options]
   (let [key-serde (:key-serde options (Serdes/String))
         value-serde (:value-serde options (Serdes/String))
         props (assoc default-pipeline-props
                      StreamsConfig/APPLICATION_ID_CONFIG
                      (str "print-topic-" (uuid)))
         builder (KStreamBuilder.)]
     (-> builder
         (ks/stream key-serde value-serde [topic-name])
         (ks/foreach (partial pprint-topic-message topic-name)))
     (new-topology props builder))))
