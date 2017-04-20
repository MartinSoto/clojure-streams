(ns clstreams.core-test
  (:require [clojure.test :refer :all]
            [clstreams.core :refer :all])
  (:import [org.apache.kafka.test ProcessorTopologyTestDriver]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]
           [org.apache.kafka.streams.kstream KStreamBuilder]))

(def string-serializer (StringSerializer.))
(def string-deserializer (StringDeserializer.))

(deftest test-word-count-topology
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
