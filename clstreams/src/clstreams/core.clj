(ns clstreams.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [signal.handler :as signal]
            [clstreams.pipelines]))

(defn run-system
  [system-var]
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

    (alter-var-root system-var component/start)

    (let [ns @next-step]

      (doseq [[sgn orig-handler] orig-handlers]
        (sun.misc.Signal/handle (signal/->signal :int) orig-handler))

      (alter-var-root system-var component/stop)

      (case ns
        :end (System/exit 0)
        :restart (recur system-var)))))


(def system nil)

(defn -main
  [func-name & args]
  (let [system-init-fn (eval (symbol func-name))]
    (alter-var-root #'system
                    (constantly (apply system-init-fn args))))
  (run-system #'system))
