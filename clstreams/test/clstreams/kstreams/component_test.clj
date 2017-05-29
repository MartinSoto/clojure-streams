(ns clstreams.kstreams.component-test
  (:require [clojure.test :refer :all]
            [clstreams.kstreams.component :as sut]
            [clstreams.testutil.component :as tu-comp]
            [clstreams.testutil.mockito :as mkto])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           org.apache.kafka.streams.KafkaStreams
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.mockito.Mockito))

(def fake-config {"ze.config.option" "ze config value"})

(def prod-topic-name "ze-topic")

(deftest test-cycle-producer-component
  (let [producer-mock (mkto/mock KafkaProducer)
        kafka-producer-fn (mkto/on-call (mkto/mock-fn) [fake-config] producer-mock)]

    (with-redefs [sut/kafka-producer kafka-producer-fn]
      (tu-comp/cycle-component

       [{:keys [config topic-name producer]} (sut/new-producer prod-topic-name fake-config)]
       ((is (= config fake-config))
        (is (= topic-name prod-topic-name))
        (is (nil? producer)))

       [{:keys [producer]}]
       ((is (= producer producer-mock)))

       [{:keys [producer]}]
       ((is (nil? producer))
        (-> producer-mock Mockito/verify .close))))))

(deftest test-idempotent-producer-component
  (let [producer-mock (mkto/mock KafkaProducer)
        kafka-producer-fn (mkto/on-call (mkto/mock-fn) [fake-config] producer-mock)]
    (with-redefs [sut/kafka-producer kafka-producer-fn]
      (tu-comp/check-idempotence (sut/new-producer prod-topic-name fake-config)))))

(deftest test-send
  (let [key "abc"
        value 42

        producer-mock (mkto/return-> (mkto/mock KafkaProducer)
                                     (.send (ProducerRecord. prod-topic-name key value))
                                     (future :the-answer))
        prod-system (assoc (sut/new-producer prod-topic-name fake-config)
                           :producer producer-mock)]

    (is (= (sut/producer-send! prod-system [key value]) [:the-answer]))))


(deftest test-cycle-topology-component
  (let [builder-mock (mkto/mock KStreamBuilder)
        kstreams-mock (mkto/mock KafkaStreams)
        kafka-streams-fn (mkto/on-call (mkto/mock-fn)
                                         [builder-mock fake-config] kstreams-mock)]

    (with-redefs [sut/kafka-streams kafka-streams-fn]
      (tu-comp/cycle-component

       [{:keys [config builder kstreams]} (sut/new-topology fake-config builder-mock)]
       ((is (= config fake-config))
        (is (= builder builder-mock))
        (is (nil? kstreams)))

       [{:keys [kstreams]}]
       ((is (= kstreams kstreams-mock)))

       [{:keys [kstreams]}]
       ((is (nil? kstreams))
        (-> kstreams-mock Mockito/verify .close))))))

(deftest test-idempotent-topology-component
  (let [builder-mock (mkto/mock KStreamBuilder)
        kstreams-mock (mkto/mock KafkaStreams)
        kafka-streams-fn (mkto/on-call (mkto/mock-fn)
                                         [builder-mock fake-config] kstreams-mock)]

    (with-redefs [sut/kafka-streams kafka-streams-fn]
      (tu-comp/check-idempotence (sut/new-topology fake-config builder-mock)))))
