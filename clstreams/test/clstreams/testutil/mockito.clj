(ns clstreams.testutil.mockito
  (:require [clojure.test :as test])
  (:import org.mockito.Mockito))

(defn mock [cls] (Mockito/mock cls))

(defn mock-fn [] (mock clojure.lang.IFn))

(defmacro return-> [mock-obj-expr method-invocation-form return-value-expr]
  `(let [mock-obj# ~mock-obj-expr]
     (-> (Mockito/doReturn ~return-value-expr)
         (.when mock-obj#)
         ~method-invocation-form)
     mock-obj#))

(defmacro on-call [mock-fn-expr parameter-expr-list return-value-expr]
  `(return-> ~mock-fn-expr (.invoke ~@parameter-expr-list) ~return-value-expr))

(defmacro verify->
  ([mock-obj-expr method-invocation-form]
   `(verify-> ~mock-obj-expr ~method-invocation-form (Mockito/times 1)))
  ([mock-obj-expr method-invocation-form counting-predicate-expr]
   `(try
      (-> ~mock-obj-expr (Mockito/verify ~counting-predicate-expr) ~method-invocation-form)
      (test/is true)
      (catch java.lang.AssertionError ae# (test/is nil (str ae#))))))

(defmacro verify-fn
  ([mock-fn-expr parameter-expr-list]
   `(verify-> ~mock-fn-expr (.invoke ~@parameter-expr-list)))
  ([mock-fn-expr parameter-expr-list counting-predicate-expr]
   `(verify-> ~mock-fn-expr (.invoke ~@parameter-expr-list) ~counting-predicate-expr)))

(defn any [] (Mockito/any))

(defn times [n] (Mockito/times n))
(defn at-least-once [] (Mockito/atLeastOnce))
(defn at-least [n] (Mockito/atLeast n))
(defn at-most [n] (Mockito/atMost n))
(defn never [] (Mockito/never))
