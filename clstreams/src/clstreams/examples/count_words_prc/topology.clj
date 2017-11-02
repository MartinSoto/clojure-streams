(ns clstreams.examples.count-words-prc.topology
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clstreams.landscape :as ldsc]
            [clstreams.processor :as prc]
            [clstreams.store :as store]
            [clstreams.topology :as topology])
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


(def word-counts-landscape
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
                     (topology/key-value-processor
                      (topology/xform-values
                       (mapcat #(str/split % #"\W+"))
                       (filter #(> (count %) 0))
                       (map str/lower-case))
                      (map (fn [[key value]] [value value])))
                     (into-array String '("src1")))
      (.addSink "snk1" "words" (into-array String '("prc1")))

      (.addSource "src2" (into-array String '("words")))
      (.addStateStore
       (store/map-store "counts")
       ;(make-counts-store)
       (into-array String '()))
      (.addProcessor "prc2"
                     (topology/transducing-processor
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
      (.connectProcessorAndStateStores "prc2" (into-array String '("counts")))
      (.addSink "snk2" "output" (into-array String '("prc2")))))

;; (println "Collector!!!" (.recordCollector ctx))


(s/check-asserts true)

(def count-words-topology
  {::prc/landscape count-words-landscape
   ::prc/nodes
   {:op01 {::prc/op ::prc/from
           ::prc/topic :file-input}
    :op02 {::prc/op ::prc/flat-map-values
           ::prc/src :op01
           ::prc/fn #(-> % str/lower-case (str/split #" +"))}
    :op03 {::prc/op ::prc/map
           ::prc/src :op02
           ::prc/fn #(vector %2 %2)}
    :op04 {::prc/op ::prc/group-by-key
           ::prc/src :op03}
    :op05 {::prc/op ::prc/count
           ::prc/src :op04
           ::prc/store "Counts"}
    :op06 {::prc/op ::prc/to
           ::prc/src :op05
           ::prc/topic :word-counts}}})

;(s/assert ::prc/topology count-words-service-raw)
