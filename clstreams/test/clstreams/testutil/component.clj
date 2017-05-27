(ns clstreams.testutil.component
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]))

(defmacro cycle-component
  "Run a component through its lifecycle (initialize, start, stop) and
  run test assertions after each step."

  [[initial-bform component-expr]
   initial-test-exprs
   [started-bform]
   started-test-exprs
   [stopped-bform]
   stopped-test-exprs]

  `(let [initial#  ~component-expr
         ~initial-bform initial#]
     ~@initial-test-exprs

     (let [started# (component/start initial#)
           ~started-bform started#]
       ~@started-test-exprs

       (let [stopped#  (component/stop started#)
             ~stopped-bform stopped#]
         ~@stopped-test-exprs))))

(defn check-idempotence
  "Check that the provided component has idempotent start and stop
  operations."

  [component]

  (cycle-component

   [initial component]
   ()

   [started]
   ((let [started-again (component/start started)]
      (is (= started started-again) "component's start is idempotent")))

   [stopped]
   ((let [stopped-again (component/stop stopped)]
      (is (= stopped stopped-again) "component's stop is idempotent")))))
