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

(defn method-wrapping-forms
  [method-name-sym param-types type-mappings]
  (let [method-call-sym (symbol (str "." (name method-name-sym)))
        param-syms (map (fn [_] (gensym "p")) param-types)]
    `([obj# ~@param-syms]
      (~method-call-sym obj#
       ~@(map (fn [typ sym] ((get type-mappings typ identity) sym))
              param-types param-syms)))))

(defn method-wrappers-map-expr
  [{:keys [members]} type-mappings]
  `(hash-map ~@(mapcat (fn [{:keys [name parameter-types]}]
                         [(clojure.core/name name)
                          `(fn ~@(method-wrapping-forms name parameter-types type-mappings))])
                       members)))

(defn add-refl-to-multimethods-data
  [initial-mm-data class-name {:keys [members]}]
  (reduce
   (fn
     [mm-data {:keys [name parameter-types return-type]}]
     (assoc-in mm-data [name [class-name (count parameter-types)]]
               {:parameter-types parameter-types
                :return-type return-type}))
   initial-mm-data
   members))

(defn add-class-to-multimethods-data [initial-mm-data class]
  (let [class-name (symbol (.getName class))
        class-refl (clojure.reflect/reflect class)]
    (add-refl-to-multimethods-data initial-mm-data class-name class-refl)))


(defn multimethod-dispatch [obj & params]
  [(class obj) (count params)])

(defn multimethod-exprs [mmethod-name mmethod-data type-mappings]
  (cons
   `(defmulti ~mmethod-name multimethod-dispatch)
   (for [[dispatch-value {:keys [parameter-types]}] mmethod-data]
     (let [[params-list method-body]
           (method-wrapping-forms mmethod-name parameter-types type-mappings)]
       `(defmethod ~mmethod-name ~dispatch-value ~params-list ~method-body)))))

(defn exprs-from-multimethods-data [mm-data type-mappings]
  (mapcat (fn [[method-name method-data]]
            (multimethod-exprs method-name method-data type-mappings)) mm-data))

(defmacro define-multimethods [mm-def-list]
  (cons 'do (eval mm-def-list)))


(defmacro iface-method-mappers [iface-symbol type-mappings-expr]
  (let [iface-refl (clojure.reflect/reflect (eval iface-symbol))
        type-mappings (eval type-mappings-expr)]
    (method-wrappers-map-expr iface-refl type-mappings)))

(def function-type-mappers
  {'org.apache.kafka.streams.kstream.KeyValueMapper
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.KeyValueMapper ~proc-expr))
   'org.apache.kafka.streams.kstream.ValueMapper
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.ValueMapper ~proc-expr))})

(def kstreams-mm-data
  (reduce add-class-to-multimethods-data
          {}
          [org.apache.kafka.streams.kstream.KStream]))

(def kstream-multimethod-defs (exprs-from-multimethods-data kstreams-mm-data
                                                            function-type-mappers))


(def kstream-operations
  (iface-method-mappers org.apache.kafka.streams.kstream.KStream function-type-mappers))
