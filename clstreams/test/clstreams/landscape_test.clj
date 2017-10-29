(ns clstreams.landscape-test
  (:require [clstreams.landscape :as sut]
            [clojure.test :refer :all]
            [clojure.spec.alpha :as s])
  (:import [org.apache.kafka.common.serialization Serdes
            StringSerializer StringDeserializer
            LongSerializer LongDeserializer]))

(deftest topic-name-test
  (is (s/valid? ::sut/topic-name "this-is-ze_topic."))
  (is (not (s/valid? ::sut/topic-name "this-is-not*ze_topic."))))

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

(deftest test-serde-getters
  (is (instance? StringSerializer
                 (sut/key-serializer count-words-landscape :text-input)))
  (is (instance? StringDeserializer
                 (sut/key-deserializer count-words-landscape :text-input)))
  (is (instance? LongDeserializer
                 (sut/value-deserializer count-words-landscape :word-counts)))
  (is (instance? LongDeserializer
                 (sut/value-deserializer count-words-landscape :word-counts))))
