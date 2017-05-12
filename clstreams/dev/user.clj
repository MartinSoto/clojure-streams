(ns user
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.string :as str]
            [clojure.test :as test]

            [clojure.tools.namespace.repl :refer [refresh]]
            [clstreams.system :refer [count-words-system]]
            [clstreams.core :refer [run-system new-control-system]]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]]))

(def system-state nil)

(defn init []
  (alter-var-root system-state
                  (constantly (count-words-system))))

(defn start []
  (alter-var-root system-state component/start))

(defn stop []
  (alter-var-root system-state
                  (fn [s] (when s (component/stop s)))))

(defn reset []
  (stop)
  (refresh :after 'user/after-reset))

(defn after-reset []
  (init)
  (start))


(defn -main
  [& _]
  (let [[func-name & args] *command-line-args*
        system-init-fn (eval (symbol func-name))
        system (apply system-init-fn args)
        control-system (new-control-system)]
    (case (run-system #'system-state system control-system)
      :end (System/exit 0)
      :restart (refresh :after 'user/-main))))
