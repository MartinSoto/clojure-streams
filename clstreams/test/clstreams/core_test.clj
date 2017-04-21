(ns clstreams.core-test
  (:require [clojure.test :refer :all]
            [clstreams.core :refer :all])
  (:import [org.apache.kafka.test ProcessorTopologyTestDriver]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]
           [org.apache.kafka.streams.kstream KStreamBuilder]))

(def string-serializer (StringSerializer.))
(def string-deserializer (StringDeserializer.))

(deftest test-copy-topology
  (testing "It works"
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
  (testing "It works"
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
      (let [record (.readOutput driver "streams-wordcount-output")]
        (println (-> driver (.getKeyValueStore "Counts") .all iterator-seq))
        (is (some? record))))))
