(ns clstreams.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [signal.handler :as signal]))

(defrecord SignalManager [action-promise]
  component/Lifecycle

  (start [component]
    (assoc component :orig-handlers
           (->
            {}
            (into (for [sgn [:term :int]]
                    [sgn
                     (signal/with-handler sgn
                       (log/debugf "Received SIG%s" (-> sgn name .toUpperCase))
                       (deliver action-promise :end))]))
            (into (for [sgn [:hup]]
                    [sgn
                     (signal/with-handler sgn
                       (log/debugf "Received SIG%s" (-> sgn name .toUpperCase))
                       (deliver action-promise :restart))])))))

  (stop [component]
    (doseq [[sgn orig-handler] (:orig-handlers component)]
      (sun.misc.Signal/handle (signal/->signal :int) orig-handler))))

(defn new-signal-manager []
  (map->SignalManager {}))


(defn new-control-system []
  (component/system-map
   :action-promise (promise)
   :signal-manager (component/using
                    (new-signal-manager)
                    [:action-promise])))


(def system-state nil)

(defn run-system
  [state-var system control-system]

  (alter-var-root state-var (constantly system))
  (alter-var-root state-var component/start)

  (let [started-control (component/start control-system)
        next-step @(:action-promise control-system)]
    (component/stop control-system)

    (alter-var-root state-var component/stop)
    next-step))

(defn -main
  [& _]
  (let [[func-name & args] *command-line-args*
        system-init-fn (eval (symbol func-name))
        system (apply system-init-fn args)
        control-system (new-control-system)]
    (case (run-system #'system-state system control-system)
      :end (System/exit 0)
      :restart (recur []))))
