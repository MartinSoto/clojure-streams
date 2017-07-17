(ns clstreams.kafka.mock-landscape-test
  (:require [clojure.test :refer :all]
            [clstreams.kafka.component :refer [get-producer! get-consumer!]]
            [clstreams.kafka.mock-landscape :as sut]))

(deftest mock-kafka-landscape
  (testing "MockKafkaLandscape"
    (let [landscape (sut/new-mock-kafka-landscape)]
      (is (instance? org.apache.kafka.clients.producer.Producer
                     (get-producer! landscape :ze-topic))
          "get-producer! returns a Kafka producer")
      (is (instance? org.apache.kafka.clients.consumer.Consumer
                     (get-consumer! landscape :ze-topic))
          "get-consumer! returns a Kafka consumer"))))
