(ns clstreams.kafka.mock-landscape-test
  (:require [clojure.test :refer :all]
            [clstreams.kafka.component :refer [get-consumer! get-producer!]]
            [clstreams.kafka.mock-landscape :as sut]
            [clstreams.testutil.component :as tu-comp]
            [clstreams.testutil.mockito :as mkto]))

(deftest test-cycle-mock-kafka-landscape
  (tu-comp/cycle-component

       [new-landscape (sut/new-mock-kafka-landscape)]
       ()

       [started-landscape]
       ()

       [stopped-landscape]
       ()))

(deftest test-assoc-or-close
  (testing "Client storage function (assoc-or-close)"
    (let [closable1 (mkto/mock java.io.Closeable)
          closable2 (mkto/mock java.io.Closeable)]
      (is (= (sut/assoc-or-close {} :kk closable1) {:kk closable1})
          "stores a new client when no client was previously stored")
      (is (= (sut/assoc-or-close {:kk closable1} :kk closable2)  {:kk closable1})
          "doesn't store a new client when a client was previously stored")
      (is (mkto/verify-> closable2 (.close))
          "closes the new client if it couldn't be stored"))
    (testing "Storing same client twice doesn't close it"
      (let [closable1 (mkto/mock java.io.Closeable)]
        (is (= (sut/assoc-or-close {:kk closable1} :kk closable1)  {:kk closable1}))
        (is (mkto/verify-> closable1 (.close) (mkto/never)))))))

(deftest mock-kafka-landscape
  (testing "MockKafkaLandscape"
    (let [landscape (sut/new-mock-kafka-landscape)
          producer (get-producer! landscape :ze-topic)
          consumer (get-consumer! landscape :ze-topic)]
      (is (instance? org.apache.kafka.clients.producer.Producer producer)
          "get-producer! returns a Kafka producer")
      (is (identical? producer (get-producer! landscape :ze-topic))
          "get-producer! returns the same object for the same topic")
      (is (not= producer (get-producer! landscape :ze-ozer-topic)))

      (is (instance? org.apache.kafka.clients.consumer.Consumer consumer)
          "get-consumer! returns a Kafka consumer")
      (is (identical? consumer (get-consumer! landscape :ze-topic))
          "get-consumer! returns the same object for the same topic")
      (is (not= consumer (get-consumer! landscape :ze-ozer-topic))))))

