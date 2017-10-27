(ns clstreams.landscape
  (:require [clojure.spec.alpha :as s])
  (:import org.apache.kafka.common.serialization.Serde))

;; TODO: Use a proper Kafka topic-name regex.
(s/def ::topic-name string?)

(s/def ::serde (partial instance? Serde))

(s/def ::value-spec (s/keys :req [::serde]))

(s/def ::type #{:stream :table})
(s/def ::keys ::value-spec)
(s/def ::values ::value-spec)

(s/def ::stream (s/keys :req [::type ::topic-name ::keys ::values]))

(s/def ::streams (s/map-of keyword? ::stream))

(s/def ::landscape (s/keys :req [::streams]))
