(ns clstreams.examples.count-words-prc.topology
  (:require [clojure.string :as str])
  (:import [org.apache.kafka.streams.processor Processor
            ProcessorSupplier StateStore StateStoreSupplier
            TopologyBuilder]))


(defn show [msg] (fn [tr] (fn [& args] (apply prn msg args) (apply tr args))))

(deftype TransducingProcessor [init-reducer-fn init-state-fn
                               ^:volatile-mutable reducer
                               ^:volatile-mutable state]

  Processor

  (init [this ctx]
    (set! reducer (init-reducer-fn ctx))
    (set! state (init-state-fn ctx)))

  (process [this key value]
    (set! state (reducer state [key value])))

  (punctuate [this timestamp] nil)

  (close [this]
    (reducer state)))

(defn transducing-processor
  ([init-reducer-fn init-state-fn]
   (reify
     ProcessorSupplier
     (get [this] (->TransducingProcessor init-reducer-fn init-state-fn nil nil)))))

(defn forward-reducer
  ([] nil)
  ([context] context)
  ([context [key value]]
   (.forward context key value)
   context))

(defn key-value-processor
  ([xform]
   (transducing-processor
    (fn [context] (xform forward-reducer))
    identity))
  ([xform1 xform2 & xforms]
   (key-value-processor (apply comp xform1 xform2 xforms))))


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
  (store-get [this key])
  (store-keys [this])
  (store-assoc! [this key value])
  (store-update! [this key update-fn])
  (store-dissoc! [this key]))

(deftype MapStore [^:volatile-mutable context st-name data]

  StateStore

  (init [this ctx root]
    (set! context ctx))

  (name [this] st-name)

  (isOpen [this] true)

  (persistent [this] false)

  (flush [this] nil)

  (close [this] nil)


  StateMap

  (store-get [this key] (get @data key))

  (store-keys [this] (keys @data))

  (store-assoc! [this key value] (swap! data assoc key value) this)

  (store-update! [this key update-fn] (swap! data update key update-fn) this)

  (store-dissoc! [this key] (swap! data dissoc key) this))

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
                   (key-value-processor
                    (xform-values
                     (mapcat #(str/split % #"\W+"))
                     (filter #(> (count %) 0))
                     (map str/lower-case))
                    (map (fn [[key value]] [value value])))
                   (into-array String '("src1")))
    (.addSink builder "snk1" "output" (into-array String '("prc1")))
    builder))
