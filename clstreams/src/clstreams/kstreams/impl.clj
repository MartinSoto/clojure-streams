(ns clstreams.kstreams.impl
  (:import [org.apache.kafka.streams.kstream])
  (:require [clojure.reflect]))

(defmacro java-function
  [iface-symbol impl-fn-expr]
  (let [iface-refl (clojure.reflect/reflect (eval iface-symbol))
        method-refl (first (:members iface-refl))
        param-syms (map (fn [_] (gensym "p")) (:parameter-types method-refl))]
    `(reify ~iface-symbol
       (~(:name method-refl) [_# ~@param-syms]
        (~impl-fn-expr ~@param-syms)))))

(defn protocol-def
  [name {:keys [members] :as iface-refl}]
  (letfn [(method-def [{:keys [name parameter-types]}]
            `(~name ~(vec (map (fn [_] (gensym)) parameter-types))))]
  `(defprotocol ~name
     ~@(map method-def members))))

(defn ks-groupByKey
  [kstream]
  (.groupByKey kstream))

(defn ks-flatMapValues
  [kstream processor]
  (.flatMapValues kstream
                  (java-function org.apache.kafka.streams.kstream.ValueMapper processor)))

(defn ks-map
  [kstream processor]
  (.map kstream
        (java-function org.apache.kafka.streams.kstream.KeyValueMapper processor)))

