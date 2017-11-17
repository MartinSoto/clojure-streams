(ns clstreams.processor
  (:import [org.apache.kafka.streams.processor Processor ProcessorSupplier]))

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


(deftype KeyValueProcessor [reducer
                            ^:volatile-mutable context]

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

(defn key-value-processor
  ([xform]
   (reify
     ProcessorSupplier
     (get [this] (->KeyValueProcessor (xform forward-reducer) nil))))
  ([xform1 xform2 & xforms]
   (key-value-processor (apply comp xform1 xform2 xforms))))


(deftype ValueProcessor [reducer
                         current-key
                         ^:volatile-mutable context]

  Processor

  (init [this ctx]
    (set! context ctx))

  (process [this key value]
    (vreset! current-key key)
    (reducer context value))

  (punctuate [this timestamp] nil)

  (close [this]
    (reducer context)))

(defn value-processor
  ([xform]
   (let [current-key (volatile! ::none)]
     (letfn [(forward-reducer
               ([] nil)
               ([context] context)
               ([context value]
                (.forward context @current-key value)
                context))]
       (reify
         ProcessorSupplier
         (get [this] (->ValueProcessor (xform forward-reducer) current-key nil))))))
  ([xform1 xform2 & xforms]
   (value-processor (apply comp xform1 xform2 xforms))))


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
