(ns clstreams.kstreams
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
