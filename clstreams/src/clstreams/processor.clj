(ns clstreams.processor
  (:require [clojure.spec.alpha :as s]
            [com.rpl.specter :as sp]))

(s/def ::op #{::count
              ::flat-map-values
              ::from
              ::group-by-key
              ::map
              ::to})

(s/def ::src keyword?)

(s/def ::topic keyword?)
;; TODO: Use a proper Kafka Streams store-name regex.
(s/def ::store string?)
(s/def ::fn fn?)

(defmulti operation-type ::op)
(defmethod operation-type ::count [_]
  (s/keys :req [::op ::src ::store]))
(defmethod operation-type ::flat-map-values [_]
  (s/keys :req [::op ::src ::fn]))
(defmethod operation-type ::from [_]
  (s/keys :req [::op ::topic]))
(defmethod operation-type ::group-by-key [_]
  (s/keys :req [::op ::src]))
(defmethod operation-type ::map [_]
  (s/keys :req [::op ::src ::fn]))
(defmethod operation-type ::to [_]
  (s/keys :req [::op ::src ::topic]))

(s/def ::operation (s/multi-spec operation-type ::op))

(s/def ::topology (s/map-of keyword? ::operation))


(defn topological-order [topology]
  (let [sources
        (seq (sp/select [sp/ALL (sp/selected? sp/LAST ::op (partial = ::from))] topology))]
    (loop [ordered sources
           ordered-ids (into #{} (sp/select [sp/ALL sp/FIRST] sources))
           remaining (apply dissoc topology ordered-ids)]
      (if-let [next-gen
               (seq (sp/select [sp/ALL (sp/selected? sp/LAST ::src ordered-ids)] remaining))]
        (let [ids (sp/select [sp/ALL sp/FIRST] next-gen)]
          (recur (concat ordered next-gen)
                 (into ordered-ids ids)
                 (apply dissoc remaining ids)))
        ordered))))
