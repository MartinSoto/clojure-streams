(ns clstreams.kstreams-test
  (:require [clojure.test :refer :all]
            [clstreams.kstreams :refer :all])
  (:import [org.apache.kafka.streams.kstream KeyValueMapper]))

(def kvmapper-reflection {:bases nil,
                          :flags #{:interface :public :abstract},
                          :members
                          #{{:name "apply",
                             :return-type java.lang.Object,
                             :declaring-class KeyValueMapper,
                             :parameter-types [java.lang.Object java.lang.Object],
                             :exception-types [],
                             :flags #{:public :abstract}}}})

(deftest test-java-function
  (testing "Can build a functional interface from an inline function"
    (let [kvm (java-function kvmapper-reflection (fn [key value] [(+ 2 key) (* 3 value)]))]
      (is (= (.apply kvm 1 2) [3 6]))))
  (testing "Can build a functional interface from a predefined function"
    (let [mapper (fn [key value] [(- 5 key) (* 2 value)])
          kvm (java-function kvmapper-reflection mapper)]
      (is (= (.apply kvm 1 2) [4 4])))))
