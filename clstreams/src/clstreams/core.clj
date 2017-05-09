(ns clstreams.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [signal.handler :as signal]
            [clstreams.pipelines]))

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

(defn new-signal-manager [subsystem action-promise]
  (component/system-map
   :subsystem subsystem
   :signal-manager (component/using
                    (map->SignalManager {:action-promise action-promise})
                    [:subsystem])))


(def system nil)

(defn run-system
  [state-var new-system-fn]
  (let [next-step (promise)
        system (new-system-fn next-step)]

      (alter-var-root state-var (constantly system))
      (alter-var-root state-var component/start)

      (let [ns @next-step]
        (alter-var-root state-var component/stop)
        ns)))

(defn -main
  [& _]
  (let [[func-name & args] *command-line-args*
        system-init-fn (eval (symbol func-name))
        system (apply system-init-fn args)]
    (case (run-system #'system (partial new-signal-manager system))
      :end (System/exit 0)
      :restart (recur []))))
