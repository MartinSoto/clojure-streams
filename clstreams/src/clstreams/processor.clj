(ns clstreams.processor
  (:require [clojure.spec.alpha :as s]
            [com.rpl.specter :as sp]
            [clstreams.landscape :as ldsc]))

(s/def ::node #{::source
                ::sink
                ::transform
                ::transform-pairs
                ::reduce})

(s/def ::preds (s/coll-of keyword? :min-count 1))
(s/def ::topic keyword?)
(s/def ::xform fn?)
(s/def ::initial (constantly true))
(s/def ::fn fn?)

(defmulti operation-type ::node)
(defmethod operation-type ::source [_]
  (s/keys :req [::node ::topic]))
(defmethod operation-type ::sink [_]
  (s/keys :req [::node ::preds ::topic]))
(defmethod operation-type ::transform [_]
  (s/keys :req [::node ::preds ::xform]))
(defmethod operation-type ::transform-pairs [_]
  (s/keys :req [::node ::preds ::xform]))
(defmethod operation-type ::reduce [_]
  (s/keys :req [::node ::preds ::initial ::fn ::topic]))

(s/def ::node-def (s/multi-spec operation-type ::node))

(s/def ::nodes (s/map-of keyword? ::node-def))

(s/def ::topology (s/keys :req [::ldsc/landscape ::nodes]))


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
