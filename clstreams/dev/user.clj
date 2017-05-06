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

   [clstreams.pipelines :refer (count-words)]))

(def system nil)

(defn init []
  (alter-var-root #'system
                  (constantly (count-words))))

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
  [func-name]
  (println "This is user.clj talking!"))
