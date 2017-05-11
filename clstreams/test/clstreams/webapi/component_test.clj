(ns clstreams.webapi.component-test
  (:require [aleph.http :as aleph]
            [clojure.test :refer :all]
            [clstreams.testutil.spies :refer [spy with-spy-redefs calls]]
            [clstreams.webapi.component :refer :all]
            [com.stuartsierra.component :as component]))

(def app-factory nil)

(deftest test-aleph-component
  (testing "Component starts and stops server"
    (let [handler (fn [& x] nil)
          [stop-server stop-server-spy] (spy)
          server-dummy (reify java.io.Closeable
                         (close [this] (stop-server this)))
          [app-factory app-factory-spy] (spy handler)]
      (with-spy-redefs
        [[aleph/start-server start-server-spy] (spy server-dummy)]

        (let [im (new-aleph app-factory {:host "0.1.2.3" :port 1234})]
          (is (= (:server im) nil))
          (is (= (-> start-server-spy calls count) 0))
          (is (= (-> stop-server-spy calls count) 0))

          (let [started-aleph (component/start im)]
            (is (= (:server started-aleph) server-dummy))
            (is (= (-> start-server-spy calls count) 1))
            (is (= (-> stop-server-spy calls count) 0))
            (is (= (-> app-factory-spy calls count) 1))
            (is (= (-> app-factory-spy calls first) [im]))

            (let [stopped-aleph (component/stop started-aleph)]
              (is (= (:stop-server stopped-aleph) nil)))))

        (is (= (-> start-server-spy calls count) 1))
        (is (= (-> start-server-spy calls first) [handler {:host "0.1.2.3" :port 1234}]))
        (is (= (-> stop-server-spy calls count) 1))
        (is (= (-> stop-server-spy calls first) [server-dummy]))))))

(comment deftest test-immutant-component
  (testing "Component starts and stops server"
    (let [handler (fn [& x] nil)
          server-dummy {:server "ze-server"}
          [app-factory app-factory-spy] (spy handler)]
      (with-spy-redefs
        [[web/run web-run-spy] (spy server-dummy)
         [web/stop web-stop-spy] (spy)]

        (let [im (new-immutant app-factory (sorted-map :host "0.1.2.3" :port 1234))]
          (is (= (:server im) nil))
          (is (= (-> web-run-spy calls count) 0))
          (is (= (-> web-stop-spy calls count) 0))

          (let [started-im (component/start im)]
            (is (= (:server started-im) server-dummy))
            (is (= (-> web-run-spy calls count) 1))
            (is (= (-> web-stop-spy calls count) 0))
            (is (= (-> app-factory-spy calls count) 1))
            (is (= (-> app-factory-spy calls first) [im]))

            (let [stopped-im (component/stop started-im)]
              (is (= (:server stopped-im) nil)))))

        (is (= (-> web-run-spy calls count) 1))
        (is (= (-> web-run-spy calls first) [handler :host "0.1.2.3":port 1234]))
        (is (= (-> web-stop-spy calls count) 1))
        (is (= (-> web-stop-spy calls first) [server-dummy]))))))
