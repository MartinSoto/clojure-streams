(ns clstreams.kstreams-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clstreams.kstreams :as ks]
            [clstreams.testutil.kstreams :refer
             [collect-output
              default-props
              key-value-map
              long-deserializer
              string-deserializer
              string-serializer]])
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.KeyValue
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.test.ProcessorTopologyTestDriver))

(defn build-copy
  [builder]
  (-> builder
    (.stream (into-array String ["streams-file-input"]))
    (.to (Serdes/String) (Serdes/String) "streams-wordcount-output")))

(deftest test-copy-topology
  (testing "Garbage in, garbage out..."
    (let [builder (KStreamBuilder.)
          _ (build-copy builder)
          driver (ProcessorTopologyTestDriver. (default-props) builder)]
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
          driver (ProcessorTopologyTestDriver. (default-props) builder)]
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
