(ns clstreams.landscape-test
  (:require [clstreams.landscape :as sut]
            [clojure.test :refer :all]
            [clojure.spec.alpha :as s])
  (:import org.apache.kafka.common.serialization.Serdes))

(def count-words-landscape
  {::sut/streams
   {:text-input {::sut/topic-name "file-input"
                 ::sut/type :stream
                 ::sut/keys {::sut/serde (Serdes/String)}
                 ::sut/values {::sut/serde (Serdes/String)}}
    :word-counts {::sut/topic-name "wordcount-output"
                  ::sut/type :table
                  ::sut/keys {::sut/serde (Serdes/String)}
                  ::sut/values {::sut/serde (Serdes/Long)}}}})

(deftest schema-test
  (is (s/valid? ::sut/landscape count-words-landscape)))
