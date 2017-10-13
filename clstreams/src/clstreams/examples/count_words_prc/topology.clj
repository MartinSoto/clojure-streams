(ns clstreams.examples.count-words-prc.topology
  (:require [clojure.string :as str])
  (:import [org.apache.kafka.streams.processor Processor ProcessorSupplier TopologyBuilder]))

(deftype WordSplitProcessor [^:volatile-mutable context]

  Processor

  (init [this ctx]
    (set! context ctx))

  (process [this key value] nil
    (doseq [word (filter #(> (count %) 0)
                         (map str/lower-case (str/split value #"\W+")))]
      (.forward context word word)))

  (punctuate [this timestamp] nil)

  (close [this] nil))

(deftype WordSplitProcessorSuppl []

  ProcessorSupplier

  (get [this] (->WordSplitProcessor nil)))

(defn build-word-count-topology []
  (let [builder (TopologyBuilder.)]
    (.addSource builder "src1" (into-array String '("input")))
    (.addProcessor builder "prc1" (->WordSplitProcessorSuppl)
                   (into-array String '("src1")))
    (.addSink builder "snk1" "output" (into-array String '("prc1")))
    builder))
