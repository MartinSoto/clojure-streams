(ns user
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.string :as str]
            [clojure.test :as test]

            [clojure.tools.namespace.repl :refer [refresh]]
            [clstreams.system :refer [count-words-system]]
            [clstreams.core :refer [run-system new-signal-manager]]
            [com.stuartsierra.component :as component]))

(def system nil)

(defn init []
  (alter-var-root #'system
                  (constantly (count-words-system))))

(defn start []
  (alter-var-root #'system component/start))

(defn stop []
  (alter-var-root #'system
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
        system (apply system-init-fn args)]
    (case (run-system #'system (partial new-signal-manager system))
      :end (System/exit 0)
      :restart (refresh :after 'user/-main))))
