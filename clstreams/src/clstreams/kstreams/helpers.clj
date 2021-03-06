(ns clstreams.kstreams.helpers
  (:require clansi
            [clojure.pprint :refer [pprint]]
            [clstreams.kafka.component :refer [new-producer new-topology]]
            [clstreams.kstreams :as ks])
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
         value-serde (:value-serde options (Serdes/String))]
     (new-producer topic-name default-producer-config
                   (.serializer key-serde) (.serializer value-serde)))))


(def default-topology-config
  {StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka:9092"
   StreamsConfig/KEY_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
   StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG 0})

(defn- pprint-topic-message [topic-name color-settings key value]
  (let [formatted-key (with-out-str (pprint key))
        formatted-value (with-out-str (pprint value))]
    (println (format "%s %s : %s"
                     (apply clansi/style (format "[%s]" topic-name) color-settings)
                     key value))))

(comment :update-me defn new-print-topic
  ([topic-name]
   (new-print-topic topic-name {}))
  ([topic-name options]
   (let [key-serde (:key-serde options (Serdes/String))
         value-serde (:value-serde options (Serdes/String))
         color-settings (:color options [])

         props (assoc default-topology-config
                      StreamsConfig/APPLICATION_ID_CONFIG
                      (str "print-topic-" topic-name))
         builder (KStreamBuilder.)]
     (-> builder
         (ks/stream key-serde value-serde [topic-name])
         (ks/foreach (partial pprint-topic-message topic-name color-settings)))
     (new-topology props builder))))
