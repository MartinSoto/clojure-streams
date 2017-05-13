(ns clstreams.kstreams.helpers
  (:require [clojure.pprint :refer [pprint]]
            [clstreams.kstreams :as ks]
            [clstreams.kstreams.component :refer [new-producer new-topology]])
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.streams.StreamsConfig))

(def default-producer-config
  {"bootstrap.servers" "kafka:9092",
   "acks" "all",
   "retries" (int 0),
   "batch.size" (int 16384),
   "linger.ms" (int 1),
   "buffer.memory" (int (* 4 1024 1024)),
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer",
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer",})

(defn new-manual-producer
  ([topic-name]
   (new-manual-producer topic-name {}))
  ([topic-name options]
   (let [key-serde (:key-serde options (Serdes/String))
         value-serde (:value-serde options (Serdes/String))
         config (assoc default-producer-config
                       "key.serializer" (-> key-serde .serializer .getClass .getName)
                       "value.serializer" (-> value-serde .serializer .getClass .getName))]
     (new-producer topic-name config))))


(def default-pipeline-props
  {StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka:9092"
   StreamsConfig/KEY_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
   StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG 0})

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn pprint-topic-message [topic-name key value]
  (let [formatted-key (with-out-str (pprint key))
        formatted-value (with-out-str (pprint value))]
    (println (format "[%s] %s : %s" topic-name key value))))

(defn new-print-topic
  ([topic-name]
   (new-print-topic topic-name {}))
  ([topic-name options]
   (let [key-serde (:key-serde options (Serdes/String))
         value-serde (:value-serde options (Serdes/String))
         props (assoc default-pipeline-props
                      StreamsConfig/APPLICATION_ID_CONFIG
                      (str "print-topic-" (uuid)))
         builder (KStreamBuilder.)]
     (-> builder
         (ks/stream key-serde value-serde [topic-name])
         (ks/foreach (partial pprint-topic-message topic-name)))
     (new-topology props builder))))