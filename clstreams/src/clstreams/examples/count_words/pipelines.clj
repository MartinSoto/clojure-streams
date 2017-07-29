(ns clstreams.examples.count-words.pipelines
  (:require [clojure.string :as str]
            [clstreams.kafka.component :refer [new-topology]]
            [clstreams.kstreams :as ks]
            [clstreams.kstreams.helpers :refer [default-topology-config]]
            [clstreams.processor :as prc]
            [clstreams.landscape :as lsc]
            [clojure.spec.alpha :as s])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KeyValue StreamsConfig]
           org.apache.kafka.streams.kstream.KStreamBuilder))

(def count-words-props
  (assoc default-topology-config StreamsConfig/APPLICATION_ID_CONFIG "streams-wordcount"))

(defn build-count-words []
  (let [builder (KStreamBuilder.)]
    (-> builder
        (ks/stream ["streams-file-input"])
        (ks/flatMapValues #(-> % str/lower-case (str/split #" +")))
        (ks/map #(KeyValue. %2 %2))
        ks/groupByKey
        (ks/count "Counts")
        (ks/to (Serdes/String) (Serdes/Long) "streams-wordcount-output"))
    builder))

(defn count-words []
  (new-topology count-words-props (build-count-words)))


(s/check-asserts true)

(def count-words-landscape
  {::lsc/streams
   {::text-input {::lsc/topic-name "file-input"
                  ::lsc/type ::lsc/stream
                  ::lsc/values {::lsc/serde (Serdes/String)}}
    ::word-counts {::lsc/topic-name "wordcount-output"
                   ::lsc/type ::lsc/table
                   ::lsc/keys {::lsc/serde (Serdes/String)}
                   ::lsc/values {::lsc/serde (Serdes/Long)}}}})

(s/assert ::lsc/landscape count-words-landscape)

;; (def count-words-processor
;;   [{::st/op ::st/from
;;     ::st/topic :file-input}
;;    {::st/op ::st/flat-map-values
;;     ::st/fn #(-> % str/lower-case (str/split #" +"))}
;;    {::st/op ::st/map
;;     ::st/fn #(KeyValue. %2 %2)}
;;    {::st/op ::st/group-by-key}
;;    {::st/op ::st/count
;;     ::st/store "Counts"}
;;    {::st/op ::st/to
;;     ::st/topic :word-counts}])

(def count-words-topology
  {:op01 {::prc/op ::prc/from
          ::prc/topic :file-input}
   :op02 {::prc/op ::prc/flat-map-values
          ::prc/src :op01
          ::prc/fn #(-> % str/lower-case (str/split #" +"))}
   :op03 {::prc/op ::prc/map
          ::prc/src :op02
          ::prc/fn #(KeyValue. %2 %2)}
   :op04 {::prc/op ::prc/group-by-key
          ::prc/src :op03}
   :op05 {::prc/op ::prc/count
          ::prc/src :op04
          ::prc/store "Counts"}
   :op06 {::prc/op ::prc/to
          ::prc/src :op05
          ::prc/topic :word-counts}})

(s/assert ::prc/topology count-words-topology)
