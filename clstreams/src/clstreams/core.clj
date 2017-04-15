(ns clstreams.core
  (:gen-class)
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream
            KStreamBuilder KStream KTable KeyValueMapper ValueMapper ForeachAction]))

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
  (doto (java.util.Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG "streams-wordcount")
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka:9092")
    (.put StreamsConfig/KEY_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName))
    (.put StreamsConfig/VALUE_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName))

    ; setting offset reset to earliest so that we can re-run the demo
    ; code with the same pre-loaded data
    ; Note: To re-run the demo, you need to use the offset reset tool:
    ; https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")))

(defn count-words
  [& args]
  (let [builder (KStreamBuilder.)
        source (.stream builder (into-array String ["streams-file-input"]))
        counts (-> source
                   (.flatMapValues
                    (reify ValueMapper
                      (apply [this value]
                        (-> value
                            str/lower-case
                            (str/split #" +")))))
                   (.map
                    (reify KeyValueMapper
                      (apply [this key value]
                        (KeyValue. value value))))
                   .groupByKey
                   (.count "Counts"))
        xxx (.to counts (Serdes/String) (Serdes/Long) "streams-wordcount-output")
        streams (KafkaStreams. builder count-words-props)]

    (.start streams)
    (Thread/sleep 5000)
    (.close streams)))

(defn print-word-counts[& args]
  (let [builder (KStreamBuilder.)
        source (.stream builder (Serdes/String) (Serdes/Long)
                        (into-array String ["streams-wordcount-output"]))
        print-counts (-> source
                         (.foreach
                          (reify ForeachAction
                            (apply [this key value]
                              (println key value)))))
        streams (KafkaStreams. builder count-words-props)]

    (.start streams)
    (Thread/sleep 5000)
    (.close streams)))
