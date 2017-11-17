(ns clstreams.examples.count-words-prc.topology
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clstreams.landscape :as ldsc]
            [clstreams.processor :as prc]
            [clstreams.store :as store]
            [clstreams.topology :as tp])
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.processor.TopologyBuilder
           org.apache.kafka.streams.state.Stores))

(defn show [msg] (fn [tr] (fn [& args] (apply prn msg args) (apply tr args))))


(defn make-counts-store []
  (-> (Stores/create "counts")
    (.withKeys (Serdes/String))
    (.withValues (Serdes/Long))
    (.persistent)
    (.build)))


(def count-words-landscape
  {::ldsc/streams
   {:input {::ldsc/topic-name "input"
            ::ldsc/type :stream
            ::ldsc/keys {::ldsc/serde (Serdes/String)}
            ::ldsc/values {::ldsc/serde (Serdes/String)}}
    :words {::ldsc/topic-name "words"
            ::ldsc/type :stream
            ::ldsc/keys {::ldsc/serde (Serdes/String)}
            ::ldsc/values {::ldsc/serde (Serdes/String)}}
    :output {::ldsc/topic-name "output"
             ::ldsc/type :table
             ::ldsc/keys {::ldsc/serde (Serdes/String)}
             ::ldsc/values {::ldsc/serde (Serdes/String)}}}})

(defn build-word-count-topology []
  (-> (TopologyBuilder.)
      (.addSource "src1" (into-array String '("input")))
      (.addProcessor "prc1"
                     (prc/value-processor
                      (mapcat #(str/split % #"\W+"))
                      (filter #(> (count %) 0))
                      (map str/lower-case))
                     (into-array String '("src1")))
      (.addProcessor "prc2"
                     (prc/key-value-processor
                      (map (fn [[key value]] [value value])))
                     (into-array String '("prc1")))
      (.addSink "snk1" "words" (into-array String '("prc2")))

      (.addSource "src2" (into-array String '("words")))
      (.addStateStore
       (store/map-store "counts")
       ;(make-counts-store)
       (into-array String '()))
      (.addProcessor "prc3"
                     (prc/transducing-processor
                      (fn [context]
                        (completing
                         (fn [store [key value]]
                           (store/store-update! store key
                                          (fn [value]
                                            (if (nil? value) 1 (inc value))))
                           (.forward context key
                                     (str (store/store-get store key)))
                           store)))
                      (fn [context] (.getStateStore context "counts")))
                     (into-array String '("src2")))
      (.connectProcessorAndStateStores "prc3" (into-array String '("counts")))
      (.addSink "snk2" "output" (into-array String '("prc3")))))

;; (println "Collector!!!" (.recordCollector ctx))


(s/check-asserts true)

(def count-words-topology
  {::ldsc/landscape count-words-landscape
   ::tp/nodes
   {:op1 {::tp/node ::tp/source
          ::tp/topic :input}
    :op2 {::tp/node ::tp/transform
          ::tp/preds [:op1]
          ::tp/xform (comp
                       (mapcat #(str/split % #"\W+"))
                       (filter #(> (count %) 0))
                       (map str/lower-case))}
    :op3 {::tp/node ::tp/transform-pairs
          ::tp/preds [:op2]
          ::tp/xform (map (fn [[key value]] [value value]))}
    :op4 {::tp/node ::tp/sink
          ::tp/preds [:op3]
          ::tp/topic :words}

    :op5 {::tp/node ::tp/source
          ::tp/topic :words}
    :op6 {::tp/node ::tp/reduce
          ::tp/preds [:op5]
          ::tp/initial 0
          ::tp/fn (fn [count word] (inc count))
          ::tp/topic :output}}})

(s/assert ::tp/topology count-words-topology)
