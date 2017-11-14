(ns clstreams.topology
  (:require [clojure.spec.alpha :as s]
            [clstreams.landscape :as ldsc]))

(s/def ::preds (s/coll-of keyword? :min-count 1 :into #{}))

(s/def ::graph-node (s/keys :opt [::preds]))
(s/def ::graph (s/map-of keyword? ::graph-node))

(s/def ::node #{::source
                ::sink
                ::transform
                ::transform-pairs
                ::reduce})

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

(s/def ::nodes (s/and ::graph (s/map-of keyword? ::node-def)))

(s/def ::topology (s/keys :req [::ldsc/landscape ::nodes]))
