(ns clstreams.webapi
  (:gen-class)
  (:require [immutant.web :as web]
            [ring.util.response :refer (response)]
            [ring.middleware.json :refer (wrap-json-body wrap-json-response)]))

(defn hello [request]
  (response {:greeting "Hello world!"}))

(defn make-app [root-handler]
  (-> root-handler
      wrap-json-body
      wrap-json-response))

(def app (make-app hello))

(defn -main []
  (let [server (web/run app :host "0.0.0.0" :port 8080)]
    (println server)
    (println "===> Running; press enter to stop")
    (read-line)
    (web/stop server)))
