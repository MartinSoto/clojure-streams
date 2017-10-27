(ns clstreams.landscape
  (:require [clojure.spec.alpha :as s])
  (:import org.apache.kafka.common.serialization.Serde))

; https://stackoverflow.com/questions/37062904/what-are-apache-kafka-topic-name-limitations
(def kafka-topic-regex #"^[a-zA-Z0-9\\._\\-]+")
(s/def ::topic-name (s/and string?
                           #(re-matches kafka-topic-regex %)
                           #(<= (count %) 249)))

(s/def ::serde (partial instance? Serde))

(s/def ::value-spec (s/keys :req [::serde]))

(s/def ::type #{:stream :table})
(s/def ::keys ::value-spec)
(s/def ::values ::value-spec)

(s/def ::stream (s/keys :req [::type ::topic-name ::keys ::values]))

(s/def ::streams (s/map-of keyword? ::stream))

(s/def ::landscape (s/keys :req [::streams]))
