(ns clstreams.core
  (:gen-class)
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream
            KStreamBuilder KStream KTable KeyValueMapper ValueMapper ForeachAction]))

(require '[clojure.string :as str])

(defn client-props []
  (let [props (java.util.Properties.)]
    (.put props "bootstrap.servers" "kafka:9092")
    (.put props "acks" "all")
    (.put props "retries" (int 0))
    (.put props "batch.size" (int 16384))
    (.put props "linger.ms" (int 1))
    (.put props "buffer.memory" (int 33554432))
    (.put props "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
    (.put props "value.serializer" "org.apache.kafka.common.serialization.StringSerializer")

    props))

(defn produce-numbered-messages
  [& args]
  (let [topic "my-topic"
        props (client-props)
        producer (KafkaProducer. props)]
    (doseq
        [n (range 100)]
      (.send producer
             (ProducerRecord. topic (format "msg%04d" n) (format "This is message %d" n))))
    (.close producer)))

(defn produce-words
  [& args]
  (let [topic "streams-file-input"
        props (client-props)
        producer (KafkaProducer. props)]
    (doto producer
      (.send (ProducerRecord. topic "" "all streams lead to kafka"))
      (.send (ProducerRecord. topic "" "hello kafka streams"))
      (.send (ProducerRecord. topic "" "join kafka summit"))
      .close)))

(defn count-words-props []
  (let [props (java.util.Properties.)]
    (.put props StreamsConfig/APPLICATION_ID_CONFIG, "streams-wordcount")
    (.put props StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, "kafka:9092")
    (.put props StreamsConfig/KEY_SERDE_CLASS_CONFIG, (-> (Serdes/String) .getClass .getName))
    (.put props StreamsConfig/VALUE_SERDE_CLASS_CONFIG, (-> (Serdes/String) .getClass .getName))

    ; setting offset reset to earliest so that we can re-run the demo
    ; code with the same pre-loaded data
    ; Note: To re-run the demo, you need to use the offset reset tool:
    ; https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
    (.put props ConsumerConfig/AUTO_OFFSET_RESET_CONFIG, "earliest")

    props))

(defn count-words
  [& args]
  (let [props (count-words-props)
        builder (KStreamBuilder.)
        source (.stream builder (into-array String ["streams-file-input"]))
        counts (-> source
                   (.flatMapValues
                    (reify ValueMapper
                      (apply [this value]
                        (-> value
                            str/lower-case
                            (str/split #" +"))
                        ;return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
                        )))
                   (.map
                    (reify KeyValueMapper
                      (apply [this key value]
                        (KeyValue. value value)
                    )))
                   .groupByKey
                   (.count "Counts"))
        xxx (.to counts (Serdes/String) (Serdes/Long) "streams-wordcount-output")
        streams (KafkaStreams. builder props)]

    (.start streams)
    (Thread/sleep 5000)
    (.close streams)))

(defn print-word-counts[& args]
  (let [props (count-words-props)
        builder (KStreamBuilder.)
        source (.stream builder (Serdes/String) (Serdes/Long)
                        (into-array String ["streams-wordcount-output"]))
        print-counts (-> source
                         (.foreach
                          (reify ForeachAction
                            (apply [this key value]
                              (println key value)
                              ))))
        streams (KafkaStreams. builder props)]

    (.start streams)
    (Thread/sleep 5000)
    (.close streams)))
