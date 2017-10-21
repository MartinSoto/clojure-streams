(ns clstreams.examples.count-words-prc.topology
  (:require [clojure.string :as str])
  (:import [org.apache.kafka.streams.processor Processor
            ProcessorSupplier StateStore StateStoreSupplier
            TopologyBuilder]))


(defn show [msg] (fn [tr] (fn [& args] (apply prn msg args) (apply tr args))))


(deftype TransducingProcessor [^:volatile-mutable context reducer]

  Processor

  (init [this ctx]
    (set! context ctx))

  (process [this key value]
    (reducer context [key value]))

  (punctuate [this timestamp] nil)

  (close [this]
    (reducer context)))

(defn forward-reducer
  ([] nil)
  ([context] context)
  ([context [key value]]
   (.forward context key value)
   context))

(defn transducing-processor
  ([xform]
   (reify
     ProcessorSupplier
     (get [this] (->TransducingProcessor nil (xform forward-reducer)))))
  ([xform1 xform2 & xforms]
   (transducing-processor (apply comp xform1 xform2 xforms))))


(defn xform-values [xform & xforms]
  (let [value-xform (apply comp xform xforms)
        current-key (volatile! ::none)]
    (letfn [(separate-key-xform [rf]
              (fn
                ([] (rf))
                ([result] (rf result))
                ([result [key value]]
                 (vreset! current-key key)
                 (rf result value))))
            (remix-key-xform [rf]
              (fn
                ([] (rf))
                ([result] (rf result))
                ([result value] (rf result [@current-key value]))))]
      (comp separate-key-xform value-xform remix-key-xform))))


(defprotocol StateMap
  (store-deref [this])
  (store-swap! [this f]))

(deftype MapStore [^:volatile-mutable context st-name state]

  StateStore

  (init [this ctx root]
    (set! context ctx))

  (name [this] st-name)

  (isOpen [this] true)

  (persistent [this] false)

  (flush [this] nil)

  (close [this] nil)


  StateMap

  (store-deref [this] @state)

  (store-swap! [this f] (swap! state f)))

(defn map-store [st-name]
  (reify StateStoreSupplier

    (get [this]
      (->MapStore nil st-name (atom {})))

    (logConfig [this] (java.util.HashMap.))

    (loggingEnabled [this] false)

    (name [this] st-name)))


(defn build-word-count-topology []
  (let [builder (TopologyBuilder.)]
    (.addSource builder "src1" (into-array String '("input")))
    (.addProcessor builder "prc1"
                   (transducing-processor
                    (xform-values
                     (mapcat #(str/split % #"\W+"))
                     (filter #(> (count %) 0))
                     (map str/lower-case))
                    (map (fn [[key value]] [value value])))
                   (into-array String '("src1")))
    (.addSink builder "snk1" "output" (into-array String '("prc1")))
    builder))
