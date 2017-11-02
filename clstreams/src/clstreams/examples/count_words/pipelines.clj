(ns clstreams.examples.count-words.pipelines
  (:require [clojure.string :as str]
            [clstreams.kafka.component :refer [new-topology]]
            [clstreams.kstreams :as ks]
            [clstreams.kstreams.helpers :refer [default-topology-config]])
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


