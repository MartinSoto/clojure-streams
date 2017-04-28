(ns clstreams.kstreams.impl-test
  (:require [clojure.test :refer :all]
            [clstreams.kstreams.impl :refer :all])
  (:import [org.apache.kafka.streams.kstream
            Aggregator KeyValueMapper KGroupedTable KTable Initializer]
           [org.apache.kafka.common.serialization Serde]))

(defmacro in-temp-ns
  [ns-name-sym run-in-ns-expr & test-assertions]
  `(let [test-ns-name# (gensym "test-ns")]
     (try
       (let [~ns-name-sym (create-ns test-ns-name#)]
         (binding [*ns* ~ns-name-sym]
           (eval '~run-in-ns-expr))
         ~@test-assertions)
       (finally (remove-ns test-ns-name#)))))

(def some-var 3)

(deftest test-in-temp-ns
  (testing "basic operation"
    (in-temp-ns
     tns
     (do
       (def aa 3)
       (def bb :xx))
     (is (= (var-get (ns-resolve tns 'aa)) 3))
     (is (= (var-get (ns-resolve tns 'bb)) :xx))))
  (testing "def does not interact with global def of same symbol"
    (in-temp-ns
     tns
     (do
       (def some-var 5))
     (is (= (var-get (ns-resolve tns 'some-var)) 5)))
    (is (= some-var 3))))

(defn val-in-ns [ns sym] (var-get (ns-resolve ns sym)))

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

(deftest test-method-wrapping-forms
  (testing "Can wrap a simple method"
    (let [wrapping-forms (method-wrapping-forms 'getOrDefault
                                                [java.lang.String java.lang.Long]
                                                {})
          wrapper (eval `(fn ~@wrapping-forms))]
      (is (= (wrapper {"a" 1} "b" 3) 3))))
  (testing "Can translate the type of a parameter"
    (let [wrapping-forms (method-wrapping-forms 'getOrDefault
                                                [java.lang.String java.lang.Long]
                                                {java.lang.Long (fn [ps] `(str ~ps))})
          wrapper (eval `(fn ~@wrapping-forms))]
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

(defn dispatch-value-class-arity
  [class-name {:keys [parameter-types return-type]}]
  [class-name (count parameter-types)])

(deftest test-add-refl-to-multimethods-data
  (testing "Builds initial data structure from simple reflection"
    (is (= (add-refl-to-multimethods-data dispatch-value-class-arity
                                          {} 'java.util.Map map-refl)
           '{getOrDefault
             {[java.util.Map 2]
              {:parameter-types [java.lang.String java.lang.Long],
               :return-type java.lang.Long}},
             get {[java.util.Map 1]
                  {:parameter-types [java.lang.String],
                   :return-type java.lang.Long}}})))
  (testing "Builds initial data structure from reflection with overloaded methods"
    (is (= (add-refl-to-multimethods-data dispatch-value-class-arity
                                          {} 'java.util.Map map-refl-overload)
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
    (let [initial (add-refl-to-multimethods-data dispatch-value-class-arity {} 'java.util.Map map-refl-overload)]
      (is (= (add-refl-to-multimethods-data dispatch-value-class-arity
                                            initial 'the.lib.MutableMap
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

(deftest test-multimethod-expr
  (testing "Builds a multimethod for one signature"
    (in-temp-ns
     test-ns
     (do
       (clojure.core/refer-clojure)
       (refer 'clstreams.kstreams.impl)

       (doseq [expr
               (multimethod-exprs 'getOrDefault
                                  {[java.util.Map 2]
                                   {:parameter-types [java.lang.String java.lang.Long],
                                    :return-type java.lang.Long}}
                                  {})]
         (eval expr))

       (def res1 (getOrDefault {"a" 1} "b" 3)))

     (is (= (val-in-ns test-ns 'res1) 3))))

  (testing "Builds a multimethod for two signatures"
    (in-temp-ns
     test-ns
     (do
       (clojure.core/refer-clojure :exclude '[replace])
       (refer 'clstreams.kstreams.impl)

       (doseq [expr
               (multimethod-exprs 'replace
                                  {[java.util.Map 2]
                                   {:parameter-types [java.lang.String java.lang.Long],
                                    :return-type java.lang.Long},
                                   [java.util.Map 3]
                                   {:parameter-types [java.lang.String java.lang.Long
                                                      java.lang.Long],
                                    :return-type java.lang.Boolean}}
                                  {})]
         (eval expr))

       (def hm (java.util.HashMap.))
       (.put hm "a" 1)
       (def res1 (replace hm "a" 3))
       (def res2 (replace hm "a" 2 5))
       (def res3 (replace hm "a" 3 5)))

     (is (= (val-in-ns test-ns 'res1) 1))
     (is (= (val-in-ns test-ns 'res2) false))
     (is (= (val-in-ns test-ns 'res3) true))))

  (testing "Builds a multimethod working on two different classes"
    (in-temp-ns
     test-ns
     (do
       (clojure.core/refer-clojure)
       (refer 'clstreams.kstreams.impl)

       (doseq [expr
               (multimethod-exprs 'size
                                  {[java.util.Map 0]
                                   {:parameter-types [],
                                    :return-type java.lang.Integer},
                                   [java.util.List 0]
                                   {:parameter-types [],
                                    :return-type java.lang.Integer}}
                                  {})]
         (eval expr))

       (def hm (java.util.HashMap.))
       (.put hm "a" 1)
       (.put hm "b" 2)
       (def ls (java.util.LinkedList.))
       (.add ls 7)
       (def res1 (size hm))
       (def res2 (size ls)))

     (is (= (val-in-ns test-ns 'res1) 2))
     (is (= (val-in-ns test-ns 'res2) 1))))

  (testing "Builds a multimethod that can translate parameters"
    (in-temp-ns
     test-ns
     (do
       (clojure.core/refer-clojure)
       (refer 'clstreams.kstreams.impl)

       (doseq [expr
               (multimethod-exprs 'add
                                  {[java.util.List 1]
                                   {:parameter-types [java.lang.String],
                                    :return-type java.lang.Boolean},
                                   [java.util.List 2]
                                   {:parameter-types [java.lang.Integer, java.lang.String],
                                    :return-type java.lang.Void}}
                                  {java.lang.String (fn [s] `(Integer/parseInt ~s))})]
         (eval expr))

       (def ls (java.util.LinkedList.))
       (add ls "20")
       (add ls "5")
       (add ls 1 "7"))

     (is (= (val-in-ns test-ns 'ls) '(20 7 5))))))
