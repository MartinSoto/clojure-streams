(ns clstreams.webapi.component
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [immutant.web :as web]))

(defrecord ImmutantWebServer [app-factory config server]
  component/Lifecycle

  (start [component]
    (log/info "Starting web server")
    (let [handler (app-factory component)
          server (apply web/run handler (mapcat seq config))]
      (assoc component :server server)))

  (stop [component]
    (log/info "Stopping web server")
    (web/stop server)
    (assoc component :server nil)))

(defn new-immutant [app-factory config]
  (map->ImmutantWebServer {:app-factory app-factory :config config}))
