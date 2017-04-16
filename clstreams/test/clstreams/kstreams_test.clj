(ns clstreams.kstreams-test
  (:require [clojure.test :refer :all]
            [clstreams.kstreams :refer :all])
  (:import [org.apache.kafka.streams.kstream Aggregator KeyValueMapper Initializer]))

(deftest test-java-function
  (testing "Can build a functional interface from an inline function"
    (let [kvm (java-function KeyValueMapper (fn [key value] [(+ 2 key) (* 3 value)]))]
      (is (= (.apply kvm 1 2) [3 6]))))
  (testing "Can build a functional interface from a predefined function"
    (let [mapper (fn [key value] [(- 5 key) (* 2 value)])
          kvm (java-function KeyValueMapper mapper)]
      (is (= (.apply kvm 1 2) [4 4]))))
  (testing "Can build a functional interface for three parameters"
    (let [agrt (java-function Aggregator (fn [key value aggr] [key (+ aggr value)]))]
      (is (= (.apply agrt 1 4 5) [1 9]))))
  (testing "Can build a functional interface for no parameters"
    (let [init (java-function Initializer (fn [] 25))]
      (is (= (.apply init) 25)))))
