(ns clstreams.system
  (:require [clstreams.pipelines :as pipelines]
            [clstreams.webapi :as webapi]
            [com.stuartsierra.component :as component]))

(defn count-words-system []
  (component/system-map
   :pipeline (pipelines/count-words)
   :webapi (component/using
            (webapi/web-test)
            [:pipeline])))
