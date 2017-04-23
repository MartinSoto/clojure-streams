(ns clstreams.kstreams
  (:refer-clojure :only [let get])
  (:require [clstreams.kstreams.impl :as impl]))

(let [ops impl/kstream-operations]
  (def groupByKey (get ops "groupByKey"))
  (def flatMapValues (get ops "flatMapValues"))
  (def map (get ops "map")))
