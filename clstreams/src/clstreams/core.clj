(ns clstreams.core
  (:gen-class)
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

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

(defn -main
  [& args]
  (let [topic "my-topic"
        props (client-props)
        producer (KafkaProducer. props)]
    (doseq
        [n (range 100)]
      (.send producer
             (ProducerRecord. topic (format "msg%04d" n) (format "This is message %d" n))))
    (.close producer)))
