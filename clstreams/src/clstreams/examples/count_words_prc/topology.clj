(ns clstreams.examples.count-words-prc.topology
  (:import org.apache.kafka.streams.processor.TopologyBuilder))

(defn build-word-count-topology []
  (let [builder (TopologyBuilder.)]
    (.addSource builder "src1" (into-array String '("input")))
    (.addSink builder "snk1" "output" (into-array String '("src1")))
    builder))
