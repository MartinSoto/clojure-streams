(ns clstreams.core
  (:gen-class)
  (:require [clojure.string :as str]
            [clstreams.kstreams :as ks]
            [com.stuartsierra.component :as component]
            [signal.handler :as signal])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder]))

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
  [& lines]
  (let [topic "streams-file-input"
        producer (KafkaProducer. client-props)]
    (doseq [line lines]
      (.send producer (ProducerRecord. topic "" line)))
    (.close producer)))


(def count-words-props
  {StreamsConfig/APPLICATION_ID_CONFIG "streams-wordcount"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka:9092"
   StreamsConfig/KEY_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
   StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG 0
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"})

(def print-word-counts-props
  (assoc count-words-props StreamsConfig/APPLICATION_ID_CONFIG "streams-printwordcounts"))

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


(defn build-count-words []
  (let [builder (KStreamBuilder.)]
    (-> builder
        (ks/stream ["streams-file-input"])
        (ks/flatMapValues #(-> % str/lower-case (str/split #" +")))
        (ks/map #(KeyValue. %2 %2))
        ks/groupByKey
        (ks/count "Counts")
        (ks/to (Serdes/String) (Serdes/Long) "streams-wordcount-output"))
    builder))

(defn build-print-word-counts []
  (let [builder (KStreamBuilder.)]
    (-> builder
        (ks/stream (Serdes/String) (Serdes/Long) ["streams-wordcount-output"])
        (ks/foreach println))
    builder))

(def count-words (new-topology count-words-props (build-count-words)))

(def print-word-counts (new-topology print-word-counts-props (build-print-word-counts)))


(defn run-system
  [system]
  (let [system-state (atom system)
        stop? (promise)]

    (signal/with-handler :int
      (println "SIGINT, bye, bye!")
      (deliver stop? true))

    (swap! system-state component/start)
    (println "Running")

    @stop?

    (println "Stopping system")
    (swap! system-state component/stop)
    (println "System stopped")
    (System/exit 0)))

(defn -main
  [func-name]
  (let [system (eval (symbol func-name))]
    (run-system system)))
