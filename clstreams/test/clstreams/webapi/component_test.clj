(ns clstreams.webapi.component-test
  (:require [clojure.test :refer :all]
            [clstreams.webapi.component :refer :all]
            [com.stuartsierra.component :as component]
            [conjure.core :as conjure]
            [immutant.web :as web]))

(def app-factory nil)

(deftest test-immutant-component
  (testing "Component starts and stops server"
    (let [handler :ze-handler
          server-dummy {:server "ze-server"}]
      (conjure/stubbing
       [web/run server-dummy
        web/stop nil
        app-factory handler]

       (let [im (new-immutant app-factory (sorted-map :host "0.1.2.3" :port 1234))]
         (is (= (:server im) nil))
         (conjure/verify-call-times-for web/run 0)
         (conjure/verify-call-times-for web/stop 0)

         (let [started-im (component/start im)]
           (is (= (:server started-im) server-dummy))
           (conjure/verify-call-times-for web/run 1)
           (conjure/verify-call-times-for web/stop 0)
           (conjure/verify-call-times-for app-factory 1)
           (conjure/verify-first-call-args-for app-factory im)

           (let [stopped-im (component/stop started-im)]
             (is (= (:server stopped-im) nil)))))

       (conjure/verify-call-times-for web/run 1)
       (conjure/verify-first-call-args-for web/run handler :host "0.1.2.3":port 1234)
       (conjure/verify-call-times-for web/stop 1)
       (conjure/verify-first-call-args-for web/stop server-dummy)))))
