(ns clstreams.kstreams.impl-test
  (:require [clojure.test :refer :all]
            [clstreams.kstreams.impl :refer :all])
  (:import [org.apache.kafka.streams.kstream
            Aggregator KeyValueMapper KGroupedTable KTable Initializer]
           [org.apache.kafka.common.serialization Serde]))

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

(deftest test-def-form-for-map-key
  (testing "Builds a definition form for a fixed map name and key"
    (is (= (def-form-for-map-key 'some-map "kkqq")
           '(def kkqq (clojure.core/get some-map "kkqq"))))))

(deftest test-ns-defs
  (testing "Builds defs for basic objects"
    (try
      (let [test-ns (create-ns 'ze-ns)]
        (binding [*ns* test-ns]
          (eval '(clojure.core/refer 'clstreams.kstreams.impl))
          (eval '(ns-defs {"a" 1 "b" 2})))
        (is (= (var-get (get (ns-interns test-ns) 'a)) 1))
        (is (= (var-get (get (ns-interns test-ns) 'b)) 2)))
      (finally (remove-ns 'ze-ns)))))

(def ktable-refl
  {:bases nil,
   :flags #{:interface :public :abstract},
   :members
   #{
     {:name 'groupBy,
      :return-type org.apache.kafka.streams.kstream.KGroupedTable,
      :declaring-class org.apache.kafka.streams.kstream.KTable,
      :parameter-types
      [org.apache.kafka.streams.kstream.KeyValueMapper
       org.apache.kafka.common.serialization.Serde
       org.apache.kafka.common.serialization.Serde],
      :exception-types [],
      :flags #{:public :abstract}}
     {:name 'through,
      :return-type org.apache.kafka.streams.kstream.KTable,
      :declaring-class org.apache.kafka.streams.kstream.KTable,
      :parameter-types
      [org.apache.kafka.common.serialization.Serde
       org.apache.kafka.common.serialization.Serde
       java.lang.String
       java.lang.String],
      :exception-types [],
      :flags #{:public :abstract}}
     {:name 'to,
      :return-type 'void,
      :declaring-class org.apache.kafka.streams.kstream.KTable,
      :parameter-types
      [org.apache.kafka.common.serialization.Serde
       org.apache.kafka.common.serialization.Serde
       java.lang.String],
      :exception-types [],
      :flags #{:public :abstract}}
   }})
