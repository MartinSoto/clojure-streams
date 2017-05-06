(ns clstreams.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [signal.handler :as signal]
            [clstreams.pipelines]))

(defn run-system
  [system-state-var]
  (let [next-step (promise)
        orig-handlers
        (->
         {}
         (into (for [sgn [:term :int]]
                 [sgn
                  (signal/with-handler sgn
                    (log/debugf "Received SIG%s" (-> sgn name .toUpperCase))
                    (deliver next-step :end))]))
         (into (for [sgn [:hup]]
                 [sgn
                  (signal/with-handler sgn
                    (log/debugf "Received SIG%s" (-> sgn name .toUpperCase))
                    (deliver next-step :restart))])))]

    (log/info "Starting topology")
    (alter-var-root system-state-var component/start)

    (let [ns @next-step]

      (doseq [[sgn orig-handler] orig-handlers]
        (sun.misc.Signal/handle (signal/->signal :int) orig-handler))

      (log/info "Stopping topology")
      (alter-var-root system-state-var component/stop)

      (case ns
        :end (System/exit 0)
        :restart (recur system-state-var)))))


(defn -main
  [func-name]
  (def system-state (eval (symbol func-name)))
  (run-system #'system-state))
