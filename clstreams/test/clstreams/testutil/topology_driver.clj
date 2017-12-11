(ns clstreams.testutil.topology-driver
  (:require [clstreams.kstreams.serdes :as serdes]
            [clstreams.landscape :as ldsc]
            [clstreams.testutil.kstreams :refer [default-props]])
  (:import org.apache.kafka.streams.kstream.KStreamBuilder
           [org.apache.kafka.streams.processor Processor TopologyBuilder]
           org.apache.kafka.streams.StreamsConfig
           org.apache.kafka.test.ProcessorTopologyTestDriver))

(defrecord TopologyTestDriver [landscape ^ProcessorTopologyTestDriver driver]
  java.io.Closeable

  (close [this] (.close driver)))

(defn test-driver
  ([landscape ^KStreamBuilder builder ^StreamsConfig props]
   (->TopologyTestDriver landscape
                         (ProcessorTopologyTestDriver. props builder)))
  ([landscape ^KStreamBuilder builder]
   (test-driver landscape builder (default-props))))

(defn process [^ProcessorTopologyTestDriver driver topic msgs]
  (let [{drv :driver landscape :landscape} driver
        topic-name (get-in landscape [::ldsc/streams topic ::ldsc/topic-name])
        key-serializer (ldsc/key-serializer landscape topic)
        value-serializer (ldsc/value-serializer landscape topic)]
    (doseq [[key value] msgs]
      (.process drv topic-name key value key-serializer value-serializer))))

(defn read-output [^ProcessorTopologyTestDriver driver topic]
  (let [{drv :driver landscape :landscape} driver
        topic-name (get-in landscape [::ldsc/streams topic ::ldsc/topic-name])
        key-deserializer (ldsc/key-deserializer landscape topic)
        value-deserializer (ldsc/value-deserializer landscape topic)]
    (if-let [record (.readOutput drv topic-name
                                 key-deserializer value-deserializer)]
      (lazy-seq (cons [(.key record) (.value record)] (read-output driver topic))))))

(defn through-kstreams-topology
  ([landscape ^KStreamBuilder builder msgs]
   (through-kstreams-topology landscape builder :input :output msgs))
  ([landscape ^KStreamBuilder builder input-topic output-topic msgs]
   (with-open [driver (test-driver landscape builder)]
     (process driver input-topic msgs)
     (read-output driver output-topic))))

(def single-processor-landscape
  {::ldsc/streams
   {:input {::ldsc/topic-name "input"
            ::ldsc/type :stream
            ::ldsc/keys {::ldsc/serde (serdes/edn-serde)}
            ::ldsc/values {::ldsc/serde (serdes/edn-serde)}}
    :output {::ldsc/topic-name "output"
             ::ldsc/type :stream
             ::ldsc/keys {::ldsc/serde (serdes/edn-serde)}
             ::ldsc/values {::ldsc/serde (serdes/edn-serde)}}}})

(defn single-processor-topology [^Processor processor]
  (let [builder (TopologyBuilder.)]
    (.addSource builder "src1"
                (ldsc/key-deserializer single-processor-landscape :input)
                (ldsc/value-deserializer single-processor-landscape :input)
                (into-array String '("input")))
    (.addProcessor builder "prc1" processor (into-array String '("src1")))
    (.addSink builder "snk1" "output"
              (ldsc/key-serializer single-processor-landscape :output)
              (ldsc/value-serializer single-processor-landscape :output)
              (into-array String '("prc1")))
    builder))

(defn through-kstreams-processor [^Processor processor msgs]
  (through-kstreams-topology single-processor-landscape
                             (single-processor-topology processor) msgs))
