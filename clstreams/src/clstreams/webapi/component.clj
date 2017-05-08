(ns clstreams.webapi.component
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [immutant.web :as web]))

(defrecord ImmutantWebServer [handler config server]
  component/Lifecycle

  (start [component]
    (log/info "Starting web server")
    (let [server (apply web/run handler (mapcat seq config))]
      (assoc component :server server)))

  (stop [component]
    (log/info "Stopping web server")
    (web/stop server)
    (assoc component :server nil)))

(defn new-immutant [handler config]
  (map->ImmutantWebServer {:handler handler :config config}))
