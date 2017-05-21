(ns clstreams.examples.game-credits.topology-test
  (:require [clstreams.examples.game-credits.state :as st]
            [clstreams.examples.game-credits.topology :as sut]
            [clstreams.kstreams.serdes :as serdes]
            [clojure.test :refer :all])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder Transformer TransformerSupplier]
           org.apache.kafka.test.ProcessorTopologyTestDriver))

(defn prod-record->map [pr]
  {:key (.key pr)
   :value (.value pr)
   :timestamp (.timestamp pr)
   :topic (.topic pr)
   :partition (.partition pr)})

(deftest test-game-credits-topology
  (let [driver (ProcessorTopologyTestDriver. (StreamsConfig. sut/game-credits-props)
                                             (sut/game-credit-builder)
                                             (into-array String ["states"]))

        states (.getStateStore driver "states")

        str-ser (-> (Serdes/String) .serializer)
        str-des (-> (Serdes/String) .deserializer)
        edn-ser (serdes/edn-serializer)
        edn-des (serdes/edn-deserializer)

        process (fn [k v] (.process driver "game-credits-requests" k v str-ser edn-ser))
        read-output (fn [] (prod-record->map
                            (.readOutput driver "game-credits-results" str-des edn-des)))

        account-key "gamer1"]

    (testing "can create an account"
      (is (nil? (.get states account-key)))

      (process account-key {:type ::st/create-account-requested})
      (let [{key :key {type :type credits :credits errors :errors} :value} (read-output)]
        (is (= key account-key))
        (is (= type ::st/account-created))
        (is (nil? credits))
        (is (nil? errors)))

      (let [{credits :credits} (.get states account-key)]
        (is (= credits 0))))))

