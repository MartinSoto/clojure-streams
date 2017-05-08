(ns clstreams.webapi
  (:require [ring.util.response :refer (response)]
            [ring.middleware.json :refer (wrap-json-body wrap-json-response)]

            [clstreams.webapi.component :refer (new-immutant)]))

(defn make-app [component]
  (-> (fn [request] (response (str (into {} component))))
      wrap-json-body
      wrap-json-response))


(defn web-test []
  (new-immutant make-app {:host "0.0.0.0" :port 8080}))
