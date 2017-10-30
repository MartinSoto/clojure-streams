(ns clstreams.examples.count-words-prc.topology
  (:require [clojure.string :as str]
            [clstreams.landscape :as ldsc]
            [clstreams.topology
             :refer
             [key-value-processor transducing-processor xform-values]]
            [clstreams.store :refer :all])
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
            ::ldsc/values {::ldsc/serde (Serdes/Long)}}
    :output {::ldsc/topic-name "output"
             ::ldsc/type :table
             ::ldsc/keys {::ldsc/serde (Serdes/String)}
             ::ldsc/values {::ldsc/serde (Serdes/Long)}}}})

(defn build-word-count-topology []
  (-> (TopologyBuilder.)
      (.addSource "src1" (into-array String '("input")))
      (.addProcessor "prc1"
                     (key-value-processor
                      (xform-values
                       (mapcat #(str/split % #"\W+"))
                       (filter #(> (count %) 0))
                       (map str/lower-case))
                      (map (fn [[key value]] [value value])))
                     (into-array String '("src1")))
      (.addSink "snk1" "words" (into-array String '("prc1")))

      (.addSource "src2" (into-array String '("words")))
      (.addStateStore
       (map-store "counts")
       ;(make-counts-store)
       (into-array String '()))
      (.addProcessor "prc2"
                     (transducing-processor
                      (fn [context]
                        (completing
                         (fn [store [key value]]
                           (store-update! store key
                                          (fn [value]
                                            (if (nil? value) 1 (inc value))))
                           (.forward context key
                                     (str (store-get store key)))
                           store)))
                      (fn [context] (.getStateStore context "counts")))
                     (into-array String '("src2")))
      (.connectProcessorAndStateStores "prc2" (into-array String '("counts")))
      (.addSink "snk2" "output" (into-array String '("prc2")))))

;; (println "Collector!!!" (.recordCollector ctx))

