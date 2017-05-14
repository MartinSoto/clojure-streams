(ns user
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.string :as str]
            [clojure.test :as test]

            [clojure.tools.namespace.repl :refer [refresh]]
            [clstreams.core :refer [run-system new-control-system]]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]]

            [clstreams.examples.count-words.system :refer
             [count-words-system produce-words]]))

(def system nil)

(defn init-system []
  (alter-var-root #'system
                  (constantly (count-words-system))))

(defn start-system []
  (alter-var-root #'system component/start))

(defn stop-system []
  (alter-var-root #'system
                  (fn [s] (when s (component/stop s)))))


(defn up []
  (when-not system
    (init-system))
  (start-system)
  nil)

(defn down []
  (when system
    (stop-system))
  nil)

(defn reset []
  (down)
  (refresh :after 'user/up))


(defn -main
  [& _]
  (let [[func-name & args] *command-line-args*
        system-init-fn (eval (symbol func-name))
        system (apply system-init-fn args)
        control-system (new-control-system)]
    (case (run-system #'system system control-system)
      :end (System/exit 0)
      :restart (refresh :after 'user/-main))))
