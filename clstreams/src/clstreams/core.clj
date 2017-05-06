(ns clstreams.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [signal.handler :as signal]
            [clstreams.pipelines]))

(defn trap-signals [action-promise]
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
              (deliver action-promise :restart))]))))

(defn untrap-signals [orig-handlers]
  (doseq [[sgn orig-handler] orig-handlers]
    (sun.misc.Signal/handle (signal/->signal :int) orig-handler)))

(defmacro run-and-handle-signals
  [start-expr stop-expr restart-expr]
  `(let [next-step# (promise)
         orig-handlers# (trap-signals next-step#)]

     ~start-expr

     (let [ns# @next-step#]

       (untrap-signals orig-handlers#)

       ~stop-expr

       (case ns#
         :end (System/exit 0)
         :restart ~restart-expr))))


(def system nil)

(defn -main
  [& _]
  (let [[func-name & args] *command-line-args*
        system-init-fn (eval (symbol func-name))]
    (alter-var-root #'system
                    (constantly (apply system-init-fn args)))
    (run-and-handle-signals
     (alter-var-root #'system component/start)
     (alter-var-root #'system component/stop)
     (recur []))))
