(ns clstreams.kstreams.component-test
  (:require [clojure.test :refer :all]
            [clstreams.kstreams.component :as sut]
            [com.stuartsierra.component :as component])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           org.mockito.Mockito))

(defn mock [cls] (Mockito/mock cls))

(defn mock-fn [] (mock clojure.lang.IFn))

(defmacro return-> [mock-obj-expr method-invocation-form return-value-expr]
  `(let [mock-obj# ~mock-obj-expr]
     (-> (Mockito/doReturn ~return-value-expr)
         (.when mock-obj#)
         ~method-invocation-form)
     mock-obj#))

(defmacro on-call-> [mock-fn-expr parameter-expr-list return-value-expr]
  `(return-> ~mock-fn-expr (.invoke ~@parameter-expr-list) ~return-value-expr))


(defmacro cycle-component
  "Run a component through its lifecycle (initialize, start, stop) and
  run test assertions after each step."

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

       (let [stopped#  (component/stop started#)
             ~stopped-bform stopped#]
         ~@stopped-test-exprs))))

(defn check-idempotence
  "Check that the provided component has idempotent start and stop
  operations."

  [component]

  (cycle-component

   [initial component]
   ()

   [started]
   ((let [started-again (component/start started)]
      (is (= started started-again) "component's start is idempotent")))

   [stopped]
   ((let [stopped-again (component/stop stopped)]
      (is (= stopped stopped-again) "component's stop is idempotent")))))


(def prod-topic-name "ze-topic")
(def prod-config {"ze.config.option" "ze config value"})

(deftest test-cycle-producer-component
  (let [producer-mock (mock KafkaProducer)
        kafka-producer-fn (on-call-> (mock-fn) [prod-config] producer-mock)]

    (with-redefs [sut/kafka-producer kafka-producer-fn]
      (cycle-component

       [{:keys [config topic-name producer]} (sut/new-producer prod-topic-name prod-config)]
       ((is (= config prod-config))
        (is (= topic-name prod-topic-name))
        (is (nil? producer)))

       [{:keys [producer]}]
       ((is (some? producer)))

       [{:keys [producer]}]
       ((is (nil? producer))
        (-> producer-mock Mockito/verify .close))))))

(deftest test-idempotent-producer-component
  (let [producer-mock (mock KafkaProducer)
        kafka-producer-fn (on-call-> (mock-fn) [prod-config] producer-mock)]
    (with-redefs [sut/kafka-producer kafka-producer-fn]
      (check-idempotence (sut/new-producer prod-topic-name prod-config)))))

(deftest test-send
  (let [key "abc"
        value 42

        producer-mock (return-> (mock KafkaProducer)
                                (.send (ProducerRecord. prod-topic-name key value))
                                (future :the-answer))
        prod-system (assoc (sut/new-producer prod-topic-name prod-config)
                           :producer producer-mock)]

    (is (= (sut/producer-send! prod-system [key value]) [:the-answer]))))
