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

(defmacro on-call-> [mock-fn-expr parameter-expr-list return-value-expr]
  `(return-> ~mock-fn-expr (.invoke ~@parameter-expr-list) ~return-value-expr))
