(ns clstreams.processor
  (:require [clojure.spec.alpha :as s]
            [clstreams.landscape :as ldsc]
            [flatland.ordered.map :refer [ordered-map]]))

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


(defn- order-from-node [ordered pending visiting node-id]
  (cond
    (ordered node-id) [ordered pending]
    (visiting node-id) (comment "Error!")
    :else
    (let [{node-pred-ids ::preds :as node-value} (pending node-id)
          visiting (conj visiting node-id)
          pending (dissoc pending node-id)]
      (let [[ordered pending]
            (loop [ordered ordered
                   pending pending
                   [pred-id & pred-ids] node-pred-ids]
              (if-not pred-id
                [ordered pending]
                (let [[ordered pending] (order-from-node ordered pending visiting pred-id)]
                  (recur ordered pending pred-ids))))]
        [(assoc ordered node-id node-value) pending]))))

(defn order-nodes
  ([nodes]
   (order-nodes (ordered-map) nodes))
  ([ordered pending]
   (let [[[node-id _] & _] (seq pending)]
     (if-not node-id
     ordered
     (let [[ordered pending] (order-from-node ordered pending #{} node-id)]
     (recur ordered pending))))))
