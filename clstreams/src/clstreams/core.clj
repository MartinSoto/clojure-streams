(ns clstreams.core
  (:gen-class)
  (:require [com.stuartsierra.component :as component]
            [signal.handler :as signal]
            [clstreams.pipelines]))

(defn run-system
  [system]
  (let [system-state (atom system)
        stop? (promise)]

    (signal/with-handler :int
      (println "SIGINT, bye, bye!")
      (deliver stop? true))

    (swap! system-state component/start)
    (println "Running")

    @stop?

    (println "Stopping system")
    (swap! system-state component/stop)
    (println "System stopped")
    (System/exit 0)))


(defn -main
  [func-name]
  (let [system (eval (symbol func-name))]
    (run-system system)))
