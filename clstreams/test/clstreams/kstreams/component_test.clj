(ns clstreams.kstreams.component-test
  (:require [clojure.test :refer :all]
            [clstreams.kstreams.component :as sut]
            [clstreams.testutil.component :as tu-comp]
            [clstreams.testutil.mockito :as mkto])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           org.mockito.Mockito))

(def prod-topic-name "ze-topic")
(def prod-config {"ze.config.option" "ze config value"})

(deftest test-cycle-producer-component
  (let [producer-mock (mkto/mock KafkaProducer)
        kafka-producer-fn (mkto/on-call-> (mkto/mock-fn) [prod-config] producer-mock)]

    (with-redefs [sut/kafka-producer kafka-producer-fn]
      (tu-comp/cycle-component

       [{:keys [config topic-name producer]} (sut/new-producer prod-topic-name prod-config)]
       ((is (= config prod-config))
        (is (= topic-name prod-topic-name))
        (is (nil? producer)))

       [{:keys [producer]}]
       ((is (= producer producer-mock)))

       [{:keys [producer]}]
       ((is (nil? producer))
        (-> producer-mock Mockito/verify .close))))))

(deftest test-idempotent-producer-component
  (let [producer-mock (mkto/mock KafkaProducer)
        kafka-producer-fn (mkto/on-call-> (mkto/mock-fn) [prod-config] producer-mock)]
    (with-redefs [sut/kafka-producer kafka-producer-fn]
      (tu-comp/check-idempotence (sut/new-producer prod-topic-name prod-config)))))

(deftest test-send
  (let [key "abc"
        value 42

        producer-mock (mkto/return-> (mkto/mock KafkaProducer)
                                     (.send (ProducerRecord. prod-topic-name key value))
                                     (future :the-answer))
        prod-system (assoc (sut/new-producer prod-topic-name prod-config)
                           :producer producer-mock)]

    (is (= (sut/producer-send! prod-system [key value]) [:the-answer]))))
