(ns clstreams.examples.count-words-prc.topology-test
  (:require [clstreams.examples.count-words-prc.topology :as sut]
            [clojure.test :refer :all]
            [clstreams.testutil.kstreams
             :refer
             [default-props string-deserializer string-serializer]])
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.streams.StreamsConfig
           org.apache.kafka.test.ProcessorTopologyTestDriver))

(defn test-driver
  ([^KStreamBuilder builder ^StreamsConfig props]
   (ProcessorTopologyTestDriver. props builder))
  ([^KStreamBuilder builder]
   (test-driver builder (default-props))))

(defn process [^ProcessorTopologyTestDriver driver topic msgs]
  (doseq [[key value] msgs]
    (.process driver topic key value string-serializer string-serializer)))

(defn read-output [^ProcessorTopologyTestDriver driver topic]
  (if-let [record (.readOutput driver topic
                               string-deserializer string-deserializer)]
    (lazy-seq (cons [(.key record) (.value record)] (read-output driver topic)))))

(defn through-kstreams-topology [^KStreamBuilder builder msgs]
  (let [driver (test-driver builder)]
      (process driver "input" msgs)
      (read-output driver "output")))

(deftest test-word-count-topology
  (let [builder (sut/build-word-count-topology)
        msgs [["" "these  are"]
              ["" " some words"]]
        expected [["these" "these"]
                  ["are" "are"]
                  ["some" "some"]
                  ["words" "words"]]]
    (is (= (through-kstreams-topology builder msgs) expected))))

