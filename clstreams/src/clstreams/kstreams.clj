(ns clstreams.kstreams
  (:refer-clojure :only [])
  (:require [clstreams.kstreams.impl :refer :all]))

(def groupByKey ks-groupByKey)
(def flatMapValues ks-flatMapValues)
(def map ks-map)

