(ns clstreams.webapi
  (:gen-class)
  (:require [immutant.web :as web]))

(defn app [request]
  {:status 200
   :body "Hello world!"})

(defn -main []
  (web/run app :host "0.0.0.0" :port 8080))
