(ns clstreams.webapi
  (:require [ring.util.response :refer (response)]
            [ring.middleware.json :refer (wrap-json-body wrap-json-response)]

            [clstreams.webapi.component :refer (new-immutant)]))

(defn hello [request]
  (response {:greeting "Hello world!"}))

(defn make-app [root-handler]
  (-> root-handler
      wrap-json-body
      wrap-json-response))

(def app (make-app hello))


(defn web-hello []
  (new-immutant app {:host "0.0.0.0" :port 8080}))
