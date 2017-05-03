(ns clstreams.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [signal.handler :as signal]
            [clstreams.pipelines]))

(defn run-system
  [system]
  (let [system-state (atom system)
        stop? (promise)]

    (signal/with-handler :int
      (log/info "Received SIGINT")
      (deliver stop? true))

    (swap! system-state component/start)
    (log/info "Topology initialized successfully")

    @stop?

    (log/info "Stopping topology")
    (swap! system-state component/stop)
    (log/info "Topology stopped")
    (System/exit 0)))


(defn -main
  [func-name]
  (let [system (eval (symbol func-name))]
    (run-system system)))
