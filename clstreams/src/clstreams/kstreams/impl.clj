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

(defn def-form-for-map-key [map-name-sym map-key]
  `(def ~(symbol map-key) (get ~map-name-sym ~map-key)))

(defmacro ns-defs
  [defs-map-expr]
  (let [defs-map (eval defs-map-expr)
        binding-sym (gensym "defs")]
    `(let [~binding-sym ~defs-map-expr]
       ~@(map (partial def-form-for-map-key binding-sym) (keys defs-map)))))

(def kstream-operations
  {"groupByKey" (fn [kstream]
                  (.groupByKey kstream))
   "flatMapValues" (fn [kstream processor]
                     (.flatMapValues kstream
                                     (java-function org.apache.kafka.streams.kstream.ValueMapper
                                                    processor)))
   "map" (fn [kstream processor]
           (.map kstream
                 (java-function org.apache.kafka.streams.kstream.KeyValueMapper processor)))})
