(ns clstreams.testutil.kstreams
  (:import [org.apache.kafka.clients.consumer ConsumerConfig]
           org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KeyValue StreamsConfig]
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.test.ProcessorTopologyTestDriver))

(def string-serializer (-> (Serdes/String) .serializer))
(def string-deserializer (-> (Serdes/String) .deserializer))

(def long-deserializer (-> (Serdes/Long) .deserializer))

(defn key-value-map [key-value-objs]
  (into {} (map (juxt #(.key %) #(.value %)) key-value-objs)))

(defn collect-output [record-producing-fn]
  (->> record-producing-fn repeatedly (take-while identity) key-value-map))


(defn default-props []
  (StreamsConfig.
   {StreamsConfig/APPLICATION_ID_CONFIG "streams-wordcount"
    StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka:9092"
    StreamsConfig/KEY_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
    StreamsConfig/VALUE_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
    ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"}))
