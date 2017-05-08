(ns clstreams.kstreams-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clstreams.kstreams :as ks])
  (:import [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder]
           [org.apache.kafka.test ProcessorTopologyTestDriver]))

(def string-serializer (-> (Serdes/String) .serializer))
(def string-deserializer (-> (Serdes/String) .deserializer))

(def long-deserializer (-> (Serdes/Long) .deserializer))

(defn key-value-map [key-value-objs]
  (into {} (map #(vector (.key %) (.value %)) key-value-objs)))

(defn collect-output [record-producing-fn]
  (->> record-producing-fn repeatedly (take-while identity) key-value-map))


(defn default-props []
  (StreamsConfig.
   {StreamsConfig/APPLICATION_ID_CONFIG "streams-wordcount"
    StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka:9092"
    StreamsConfig/KEY_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
    StreamsConfig/VALUE_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
    ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"}))

(defn build-copy
  [builder]
  (-> builder
    (.stream (into-array String ["streams-file-input"]))
    (.to (Serdes/String) (Serdes/String) "streams-wordcount-output")))

(deftest test-copy-topology
  (testing "Garbage in, garbage out..."
    (let [builder (KStreamBuilder.)
          _ (build-copy builder)
          driver (ProcessorTopologyTestDriver. (default-props) builder
                                               (into-array String ["Counts"]))]
      (.process driver "streams-file-input" "" "all streams lead to kafka"
                string-serializer string-serializer)
      (let [record (.readOutput driver "streams-wordcount-output"
                                string-deserializer string-deserializer)]
        (is (= (.key record) ""))
        (is (= (.value record) "all streams lead to kafka"))))))

(defn build-count-words
  [builder]
  (-> builder
      (ks/stream ["streams-file-input"])
      (ks/flatMapValues #(-> % str/lower-case (str/split #" +")))
      (ks/map #(KeyValue. %2 %2))
      ks/groupByKey
      (ks/count "Counts")
      (ks/to (Serdes/String) (Serdes/Long) "streams-wordcount-output")))

(deftest test-word-count-topology
  (testing "Complete table written to output topic"
    (let [builder (KStreamBuilder.)
          _ (build-count-words builder)
          driver (ProcessorTopologyTestDriver. (default-props) builder
                                               (into-array String ["Counts"]))]
      (.process driver "streams-file-input" "" "all streams lead to kafka"
                string-serializer string-serializer)
      (.process driver "streams-file-input" "" "hello kafka streams"
                string-serializer string-serializer)
      (.process driver "streams-file-input" "" "join kafka summit"
                string-serializer string-serializer)
      (let [output (collect-output #(.readOutput driver "streams-wordcount-output"
                                                 string-deserializer long-deserializer))]
        (let [expected {"all" 1, "streams" 2, "lead" 1, "to" 1,
                        "kafka" 3, "hello" 1, "join" 1, "summit" 1}]
          (is (= output expected) "Output topic contents are correct")
          (is (= (-> driver (.getKeyValueStore "Counts") .all iterator-seq key-value-map)
                 expected) "Store contents are correct"))))))
