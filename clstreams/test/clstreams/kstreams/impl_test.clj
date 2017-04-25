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
    (let [test-ns-name (gensym "test-ns")]
      (try
        (let [test-ns (create-ns test-ns-name)]
          (binding [*ns* test-ns]
            (eval '(do
                     (clojure.core/refer 'clstreams.kstreams.impl)
                     (ns-defs {"a" 1 "b" 2}))))
          (is (= (var-get (get (ns-interns test-ns) 'a)) 1))
          (is (= (var-get (get (ns-interns test-ns) 'b)) 2)))
        (finally (remove-ns test-ns-name))))))

(deftest test-method-wrapper-expr
  (testing "Can wrap a simple method"
    (let [wrapper-code (method-wrapper-expr 'getOrDefault
                                            [java.lang.String java.lang.Long]
                                            {})
          wrapper (eval wrapper-code)]
      (is (= (wrapper {"a" 1} "b" 3) 3))))
  (testing "Can translate the type of a parameter"
    (let [wrapper-code (method-wrapper-expr 'getOrDefault
                                            [java.lang.String java.lang.Long]
                                            {java.lang.Long (fn [ps] `(str ~ps))})
          wrapper (eval wrapper-code)]
      (is (= (wrapper {"a" 1} "b" 3) "3")))))

(def map-refl
  '{:members
    #{{:name get
       :parameter-types [java.lang.String]
       :return-type java.lang.Long}
      {:name getOrDefault
       :parameter-types [java.lang.String java.lang.Long]
       :return-type java.lang.Long}}})

(def map-refl-overload
  '{:members
    #{{:name get
       :parameter-types [java.lang.String java.lang.Integer]
       :return-type java.lang.Integer}
      {:name get
       :parameter-types [java.lang.String]
       :return-type java.lang.Long}
      {:name getOrDefault
       :parameter-types [java.lang.String java.lang.Long]
       :return-type java.lang.Long}}})

(def mutable-map-refl-overload
  '{:members
    #{{:name get
       :parameter-types [java.lang.String]
       :return-type java.lang.Long}
      {:name put
       :parameter-types [java.lang.String java.lang.Long]
       :return-type java.lang.Long}}})

(deftest test-method-wrappers-map-expr
  (testing "Builds a wrapper map for a full class reflexion"
    (let [map-code (method-wrappers-map-expr map-refl
                                             {'java.lang.Long (fn [ps] `(str ~ps))})
          method-wrappers (eval map-code)]
      (is (= (set (keys method-wrappers)) #{"get" "getOrDefault"}))
      (is (= ((method-wrappers "get") {"a" 1} "a") 1))
      (is (= ((method-wrappers "getOrDefault") {"a" 1} "b" 3) "3")))))

(deftest test-add-refl-to-multimethods-data
  (testing "Builds initial data structure from simple reflection"
    (is (= (add-refl-to-multimethods-data {} 'java.util.Map map-refl)
           '{getOrDefault
             {[java.util.Map 2]
              {:parameter-types [java.lang.String java.lang.Long],
               :return-type java.lang.Long}},
             get {[java.util.Map 1]
                  {:parameter-types [java.lang.String],
                   :return-type java.lang.Long}}})))
  (testing "Builds initial data structure from reflection with overloaded methods"
    (is (= (add-refl-to-multimethods-data {} 'java.util.Map map-refl-overload)
           '{getOrDefault
             {[java.util.Map 2]
              {:parameter-types [java.lang.String java.lang.Long],
               :return-type java.lang.Long}},
             get
             {[java.util.Map 1]
              {:parameter-types [java.lang.String], :return-type java.lang.Long},
              [java.util.Map 2]
              {:parameter-types [java.lang.String java.lang.Integer],
               :return-type java.lang.Integer}}})))
  (testing "Extends data structure with methods from a second class"
    (let [initial (add-refl-to-multimethods-data {} 'java.util.Map map-refl-overload)]
      (is (= (add-refl-to-multimethods-data initial 'the.lib.MutableMap
                                            mutable-map-refl-overload)
             '{getOrDefault
               {[java.util.Map 2]
                {:parameter-types [java.lang.String java.lang.Long],
                 :return-type java.lang.Long}},
               get
               {[java.util.Map 1]
                {:parameter-types [java.lang.String], :return-type java.lang.Long},
                [java.util.Map 2]
                {:parameter-types [java.lang.String java.lang.Integer],
                 :return-type java.lang.Integer},
                [the.lib.MutableMap 1]
                {:parameter-types [java.lang.String], :return-type java.lang.Long}},
               put
               {[the.lib.MutableMap 2]
                {:parameter-types [java.lang.String java.lang.Long],
                 :return-type java.lang.Long}}})))))

