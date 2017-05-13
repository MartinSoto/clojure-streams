(ns clstreams.examples.count-words.pipelines
  (:require [clojure.string :as str]
            [clstreams.kstreams :as ks]
            [clstreams.kstreams.component :refer [new-topology]]
            [clstreams.kstreams.helpers :refer [default-pipeline-props]])
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.KeyValue
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.streams.StreamsConfig))

(def count-words-props
  (assoc default-pipeline-props StreamsConfig/APPLICATION_ID_CONFIG "streams-wordcount"))

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
