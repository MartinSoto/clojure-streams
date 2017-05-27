(ns clstreams.kstreams.component-test
  (:require [clstreams.kstreams.component :as sut]
            [clojure.test :refer :all]
            [com.stuartsierra.component :as component])
  (:import org.apache.kafka.clients.producer.MockProducer
           org.apache.kafka.common.serialization.Serdes))

(defn kafka-mock-producer
  ([config] (MockProducer. true
                           (.serializer (Serdes/String))
                           (.serializer (Serdes/String))))
  ([config key-serde value-serde] (MockProducer. true
                                                 (.serializer key-serde)
                                                 (.serializer value-serde))))

(defmacro cycle-component
  [[initial-bform component-expr]
   initial-test-exprs
   [started-bform]
   started-test-exprs
   [stopped-bform]
   stopped-test-exprs]

  `(let [initial#  ~component-expr
         ~initial-bform initial#]
     ~@initial-test-exprs

     (let [started# (component/start initial#)
           ~started-bform started#]
       ~@started-test-exprs

       (let [stopped#  (component/stop ~started-bform)
             ~stopped-bform stopped#]
         ~@stopped-test-exprs))))


(deftest test-producer-component
  (with-redefs [sut/kafka-producer kafka-mock-producer]
    (let [tp-name "ze-topic"
          cnf {"ze.config.option" "ze config value"}]
      (cycle-component
       [{:keys [config topic-name producer]} (sut/new-producer tp-name cnf)]
       ((is (= config cnf))
        (is (= topic-name tp-name))
        (is (nil? producer)))

       [{:keys [producer]}]
       ((is (some? producer)))

       [{:keys [producer]}]
       ((is (nil? producer)))))))
