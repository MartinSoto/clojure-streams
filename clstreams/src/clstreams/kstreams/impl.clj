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


(defn method-wrapping-forms
  [method-name-sym param-types type-mappings]
  (let [method-call-sym (symbol (str "." (name method-name-sym)))
        param-syms (map (fn [_] (gensym "p")) param-types)]
    `([obj# ~@param-syms]
      (~method-call-sym obj#
       ~@(map (fn [typ sym] ((get type-mappings typ identity) sym))
              param-types param-syms)))))


(defn add-refl-to-multimethods-data
  [dispatch-value-fn initial-mm-data class-name {:keys [members]}]
  (reduce
   (fn
     [mm-data {:keys [name parameter-types return-type] :as mmethod}]
     (update-in mm-data [name (dispatch-value-fn class-name mmethod)]
                (fn [old]
                  (if old
                    (do
                      (printf "Dispatch value conflict in %s.%s\n" class-name name)
                      old)
                    {:parameter-types parameter-types
                     :return-type return-type}))))
   initial-mm-data
   members))

(defn add-class-to-multimethods-data [dispatch-value-fn initial-mm-data class]
  (let [class-name (symbol (.getName class))
        class-refl (clojure.reflect/reflect class)]
    (add-refl-to-multimethods-data dispatch-value-fn initial-mm-data class-name class-refl)))


(defn multimethod-dispatch [& params]
  (vec (map class params)))

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


(def function-type-mappers
  {'org.apache.kafka.streams.kstream.KeyValueMapper
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.KeyValueMapper ~proc-expr))
   'org.apache.kafka.streams.kstream.ValueMapper
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.ValueMapper ~proc-expr))})

(defn dispatch-value-from-param [param-type]
  (cond
    (clojure.string/ends-with? (str param-type) "<>") 'clojure.lang.ISeq
    (contains? function-type-mappers param-type) 'clojure.lang.IFn
    :else param-type))

(defn dispatch-value-from-params
  [class-name-sym {:keys [parameter-types return-type]}]
  (vec (cons class-name-sym
             (map dispatch-value-from-param parameter-types))))

(def kstreams-mm-data
  (reduce (partial add-class-to-multimethods-data dispatch-value-from-params)
          {}
          [org.apache.kafka.streams.kstream.KStream]))

(def kstream-multimethod-defs (exprs-from-multimethods-data kstreams-mm-data
                                                            function-type-mappers))
