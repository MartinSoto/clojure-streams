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
  (when (some? pr)
    {:key (.key pr)
     :value (.value pr)
     :timestamp (.timestamp pr)
     :topic (.topic pr)
     :partition (.partition pr)}))

(deftest test-game-credits-topology
  (let [store-name "states"

        driver (ProcessorTopologyTestDriver. (StreamsConfig. sut/game-credits-props)
                                             (sut/game-credit-builder))

        states (.getKeyValueStore driver store-name)

        str-ser (-> (Serdes/String) .serializer)
        str-des (-> (Serdes/String) .deserializer)
        edn-ser (serdes/edn-serializer)
        edn-des (serdes/edn-deserializer)

        process (fn [k v] (.process driver "game-credits-requests" k v str-ser edn-ser))

        read-error-event
        (fn [] (prod-record->map
                (.readOutput driver
                             "game-credits-requests-errors"
                             str-des edn-des)))

        account-key "gamer1"]

    (testing "can create an account"
      (is (nil? (.get states account-key)))

      (process account-key {:type ::st/create-account-requested})
      (let [{:keys [type balance credits errors] :as state} (.get states account-key)]
        (is (some? state))

        (is (= type ::st/account-created))
        (is (= balance 0))
        (is (nil? credits))
        (is (nil? errors)))

      (is (nil? (read-error-event)) "no error was produced"))

    (testing "using more credits than available results in an error"
      (let [state {:balance 7}]
        (.put states account-key state)

        (process account-key {:type ::st/use-credits-requested
                              :credits 10})
        (let [{key :key {:keys [type balance credits errors]} :value :as out} (read-error-event)]
          (is (some? out))

          (is (= key account-key))

          (is (= type ::st/insufficient-credits-error))
          (is (nil? balance))
          (is (nil? credits))
          (is (some? errors)))

        (is (= (.get states account-key) state) "state wasn't changed")))))

