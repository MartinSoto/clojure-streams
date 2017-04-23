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

(defn method-wrapper-expr
  [method-name-sym param-types type-mappings]
  (let [method-call-sym (symbol (str "." (name method-name-sym)))
        param-syms (map (fn [_] (gensym "p")) param-types)]
    `(fn [obj# ~@param-syms]
       (~method-call-sym obj#
        ~@(map (fn [typ sym] ((get type-mappings typ identity) sym))
               param-types param-syms)))))

(defn method-wrappers-map-expr
  [{:keys [members]} type-mappings]
  `(hash-map ~@(mapcat (fn [{:keys [name parameter-types]}]
                         [(clojure.core/name name)
                          (method-wrapper-expr name parameter-types type-mappings)])
                       members)))

(defmacro iface-method-mappers [iface-symbol type-mappings-expr]
  (let [iface-refl (clojure.reflect/reflect (eval iface-symbol))
        type-mappings (eval type-mappings-expr)]
    (method-wrappers-map-expr iface-refl type-mappings)))


(def function-type-mappers
  {'org.apache.kafka.streams.kstream.KeyValueMapper
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.KeyValueMapper ~proc-expr))
   'org.apache.kafka.streams.kstream.ValueMapper
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.ValueMapper ~proc-expr))})

(def kstream-operations
  (iface-method-mappers org.apache.kafka.streams.kstream.KStream function-type-mappers))

