(ns clstreams.core-test
  (:require [clojure.test :refer :all]
            [clstreams.core :refer :all])
  (:import [org.apache.kafka.test ProcessorTopologyTestDriver]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams.kstream KStreamBuilder]))

(def string-serializer (-> (Serdes/String) .serializer))
(def string-deserializer (-> (Serdes/String) .deserializer))

(def long-deserializer (-> (Serdes/Long) .deserializer))

(defn key-value-map [key-value-objs]
  (into {} (map #(vector (.key %) (.value %)) key-value-objs)))

(defn collect-output [record-producing-fn]
  (->> record-producing-fn repeatedly (take-while identity) key-value-map))

(deftest test-copy-topology
  (testing "Garbage in, garbage out..."
    (let [builder (KStreamBuilder.)
          _ (build-copy builder)
          driver (ProcessorTopologyTestDriver. count-words-props builder
                                               (into-array String ["Counts"]))]
      (.process driver "streams-file-input" "" "all streams lead to kafka"
                string-serializer string-serializer)
      (let [record (.readOutput driver "streams-wordcount-output"
                                string-deserializer string-deserializer)]
        (is (= (.key record) ""))
        (is (= (.value record) "all streams lead to kafka"))))))

(deftest test-word-count-topology
  (testing "Complete table written to output topic"
    (let [builder (KStreamBuilder.)
          _ (build-count-words builder)
          driver (ProcessorTopologyTestDriver. count-words-props builder
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
