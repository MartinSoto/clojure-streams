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
  [method-name-sym param-types param-wrapper-for-type]
  (let [method-call-sym (symbol (str "." (name method-name-sym)))
        param-syms (map (fn [_] (gensym "p")) param-types)]
    `([obj# ~@param-syms]
      (~method-call-sym obj#
       ~@(map (fn [typ sym] ((param-wrapper-for-type typ) sym)) param-types param-syms)))))


(defn add-refl-to-multimethods-data
  [dispatch-value-fn initial-mm-data class-name {:keys [members]}]
  (->>
   members
   (remove
    #(clojure.string/includes? (str (:name %)) "."))
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
    initial-mm-data)))

(defn add-class-to-multimethods-data [dispatch-value-fn initial-mm-data class]
  (let [class-name (symbol (.getName class))
        class-refl (clojure.reflect/reflect class)]
    (add-refl-to-multimethods-data dispatch-value-fn initial-mm-data class-name class-refl)))


(defn multimethod-dispatch [& params]
  (vec (map class params)))

(defn multimethod-exprs [mmethod-name mmethod-data param-wrapper-for-type]
  (cons
   `(defmulti ~mmethod-name multimethod-dispatch)
   (for [[dispatch-value {:keys [parameter-types]}] mmethod-data]
     (let [[params-list method-body]
           (method-wrapping-forms mmethod-name parameter-types param-wrapper-for-type)]
       `(defmethod ~mmethod-name ~dispatch-value ~params-list ~method-body)))))

(defn exprs-from-multimethods-data [mm-data param-wrapper-for-type]
  (mapcat (fn [[method-name method-data]]
            (multimethod-exprs method-name method-data param-wrapper-for-type)) mm-data))


(def function-type-param-wrappers
  {'org.apache.kafka.streams.kstream.Aggregator
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.Aggregator ~proc-expr))
   'org.apache.kafka.streams.kstream.ForeachAction
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.ForeachAction ~proc-expr))
   'org.apache.kafka.streams.kstream.Initializer
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.Initializer ~proc-expr))
   'org.apache.kafka.streams.kstream.KeyValueMapper
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.KeyValueMapper ~proc-expr))
   'org.apache.kafka.streams.kstream.Merger
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.Merger ~proc-expr))
   'org.apache.kafka.streams.kstream.Predicate
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.Predicate ~proc-expr))
   'org.apache.kafka.streams.kstream.Reducer
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.Reducer ~proc-expr))
   'org.apache.kafka.streams.kstream.ValueJoiner
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.ValueJoiner ~proc-expr))
   'org.apache.kafka.streams.kstream.ValueMapper
   (fn [proc-expr] `(java-function org.apache.kafka.streams.kstream.ValueMapper ~proc-expr))})

(defn dispatch-value-from-param [param-type-sym]
  (cond
    (clojure.string/ends-with? (str param-type-sym) "<>") 'clojure.lang.Seqable
    (contains? function-type-param-wrappers param-type-sym) 'clojure.lang.IFn
    :else param-type-sym))

(defn dispatch-value-from-params
  [class-name-sym {:keys [parameter-types return-type]}]
  (vec (cons class-name-sym
             (map dispatch-value-from-param parameter-types))))

(defn param-wrapper-for-type
  [param-type-sym]
  (let [type-name (str 'String<>)]
    (if (clojure.string/ends-with? (str param-type-sym) "<>")
      (let [elem-type-sym (symbol (subs type-name 0 (- (count type-name) 2)))]
        (fn [seq-expr] `(into-array ~elem-type-sym ~seq-expr)))
      (get function-type-param-wrappers param-type-sym identity))))

(def kstreams-mm-data
  (reduce (partial add-class-to-multimethods-data dispatch-value-from-params)
          {}
          [org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.streams.kstream.KStream
           org.apache.kafka.streams.kstream.KGroupedStream
           org.apache.kafka.streams.kstream.KTable
           org.apache.kafka.streams.kstream.KGroupedTable]))

(def kstream-multimethod-defs (exprs-from-multimethods-data kstreams-mm-data
                                                            param-wrapper-for-type))

(defmacro define-multimethods []
  (cons 'do kstream-multimethod-defs))
