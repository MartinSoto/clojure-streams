(ns clstreams.store
  (:import [org.apache.kafka.streams.processor StateStore StateStoreSupplier]))

(defprotocol StateMap
  (store-get [this key])
  (store-keys [this])
  (store-assoc! [this key value])
  (store-update! [this key update-fn])
  (store-dissoc! [this key]))

(deftype MapStore [^:volatile-mutable context st-name data]

  StateStore

  (init [this ctx root]
    (set! context ctx)
    (.register ctx root false nil))

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
