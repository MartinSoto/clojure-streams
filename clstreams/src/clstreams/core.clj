(ns clstreams.core
  (:gen-class)
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream
            KStreamBuilder KStream KTable KeyValueMapper ValueMapper ForeachAction])
  (:require [clstreams.kstreams :as ks]))

(require '[clojure.string :as str])

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

(defn produce-numbered-messages
  [& args]
  (let [topic "my-topic"
        producer (KafkaProducer. client-props)]
    (doseq
        [n (range 100)]
      (.send producer
             (ProducerRecord. topic (format "msg%04d" n) (format "This is message %d" n))))
    (.close producer)))

(defn produce-words
  [& args]
  (let [topic "streams-file-input"
        producer (KafkaProducer. client-props)]
    (doto producer
      (.send (ProducerRecord. topic "" "all streams lead to kafka"))
      (.send (ProducerRecord. topic "" "hello kafka streams"))
      (.send (ProducerRecord. topic "" "join kafka summit"))
      .close)))

(def count-words-props
  (StreamsConfig.
   {StreamsConfig/APPLICATION_ID_CONFIG "streams-wordcount"
    StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka:9092"
    StreamsConfig/KEY_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
    StreamsConfig/VALUE_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
    ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"}))

(defn run-topology
  [build-topology-fn props]
  (let [builder (KStreamBuilder.)
        _ (build-topology-fn builder)
        streams (KafkaStreams. builder count-words-props)]
    (.start streams)
    (Thread/sleep 5000)
    (.close streams)))

(defn build-copy
  [builder]
  (-> builder
    (.stream (into-array String ["streams-file-input"]))
    (.to (Serdes/String) (Serdes/String) "streams-wordcount-output")))

(defn build-count-words
  [builder]
  (-> builder
      (ks/stream ["streams-file-input"])
      (ks/flatMapValues #(-> % str/lower-case (str/split #" +")))
      (ks/map #(KeyValue. %2 %2))
      ks/groupByKey
      (ks/count "Counts")
      (ks/to (Serdes/String) (Serdes/Long) "streams-wordcount-output")))

(defn count-words
  [& args]
  (run-topology build-count-words count-words-props))

(defn print-word-counts[& args]
  (let [builder (KStreamBuilder.)
        source (.stream builder (Serdes/String) (Serdes/Long)
                        (into-array String ["streams-wordcount-output"]))
        print-counts (-> source
                         (ks/foreach #(println %1 %2)))
        streams (KafkaStreams. builder count-words-props)]

    (.start streams)
    (Thread/sleep 5000)
    (.close streams)))
