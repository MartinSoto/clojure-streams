(ns clstreams.examples.count-words.system
  (:require [clstreams.examples.count-words.pipelines :as pipelines]
            [clstreams.examples.count-words.webapi :as webapi]
            [com.stuartsierra.component :as component]))

(defn count-words-system []
  (component/system-map
   :pipeline (pipelines/count-words)
   :webapi (component/using
            (webapi/web-test)
            [:pipeline])))
