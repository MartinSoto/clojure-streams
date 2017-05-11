(ns clstreams.webapi.component
  (:require [aleph.http :as aleph]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]))

(defrecord AlephWebServer [app-factory config server]
  component/Lifecycle

  (start [component]
    (log/info "Starting web server")
    (let [handler (app-factory component)
          server (aleph/start-server handler config)]
      (assoc component :stop-server server)))

  (stop [component]
    (log/info "Stopping web server")
    (:stop-server component)
    (assoc component :server nil)))

(defn new-aleph [app-factory config]
  (map->AlephWebServer {:app-factory app-factory :config config}))


(comment defrecord ImmutantWebServer [app-factory config server]
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

(comment defn new-immutant [app-factory config]
  (map->ImmutantWebServer {:app-factory app-factory :config config}))
