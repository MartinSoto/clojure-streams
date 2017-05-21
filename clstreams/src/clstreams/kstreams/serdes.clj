(ns clstreams.kstreams.serdes
  (:require [clojure.edn :refer [read-string]])
  (:import [org.apache.kafka.common.serialization Deserializer
            Serde Serializer StringDeserializer StringSerializer]))

(deftype EDNSerializer [str-serializer]

  Serializer

  (configure [this configs is-key] nil)

  (serialize [this topic data]
    (->> data
         (pr-str)
         (.serialize str-serializer topic)))

  (close [this] nil))

(defn edn-serializer []
  (->EDNSerializer (StringSerializer.)))


(deftype EDNDeserializer [str-deserializer]

  Deserializer

  (configure [this configs is-key] nil)

  (deserialize [this topic data]
    (->> data
         (.deserialize str-deserializer topic)
         (read-string)))

  (close [this] nil))

(defn edn-deserializer []
  (->EDNDeserializer (StringDeserializer.)))


(deftype EDNSerde [ser des]

  Serde

  (configure [this configs is-key] nil)

  (serializer [this] ser)

  (deserializer [this] des)

  (close [this] nil))

(defn edn-serde []
  (->EDNSerde (edn-serializer) (edn-deserializer)))
