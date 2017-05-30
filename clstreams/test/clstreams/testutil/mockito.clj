(ns clstreams.testutil.mockito
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
   `(-> ~mock-obj-expr (Mockito/verify ~counting-predicate-expr) ~method-invocation-form)))

(defmacro verify-fn
  ([mock-fn-expr parameter-expr-list]
   `(verify-> ~mock-fn-expr (.invoke ~@parameter-expr-list)))
  ([mock-fn-expr parameter-expr-list counting-predicate-expr]
   `(verify-> ~mock-fn-expr (.invoke ~@parameter-expr-list) ~counting-predicate-expr)))

(defn times [n] (Mockito/times n))
(defn at-least-once [] (Mockito/atLeastOnce))
(defn at-least [n] (Mockito/atLeast n))
(defn at-most [n] (Mockito/atMost n))
(defn never [] (Mockito/never))
