(ns user
  (:gen-class)
  (:require
   [clojure.java.io :as io]
   [clojure.pprint :refer (pprint)]
   [clojure.repl :refer :all]
   [clojure.string :as str]
   [clojure.test :as test]
   [clojure.tools.namespace.repl :refer (refresh refresh-all)]

   [com.stuartsierra.component :as component]

   [clstreams.core :refer (run-and-handle-signals)]
   [clstreams.system :refer (count-words-system)]))

(def system nil)

(defn init []
  (alter-var-root #'system
                  (constantly (count-words-system))))

(defn start []
  (alter-var-root #'system component/start))

(defn stop []
  (alter-var-root #'system
                  (fn [s] (when s (component/stop s)))))

(defn go []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh :after 'user/go))


(defn -main
  [& _]
  (let [[func-name & args] *command-line-args*
        system-init-fn (eval (symbol func-name))]
    (alter-var-root #'system
                    (constantly (apply system-init-fn args)))
    (run-and-handle-signals
     (alter-var-root #'system component/start)
     (alter-var-root #'system component/stop)
     (refresh :after 'user/-main))))
