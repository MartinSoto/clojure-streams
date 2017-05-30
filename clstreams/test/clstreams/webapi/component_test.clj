(ns clstreams.webapi.component-test
  (:require [aleph.http :as aleph]
            [clojure.test :refer :all]
            [clstreams.testutil.component :as tu-comp]
            [clstreams.testutil.mockito :as mkto]
            [clstreams.webapi.component :refer :all])
  (:import org.mockito.Mockito))

(def app-factory nil)

(deftest test-cycle-aleph-component
  (testing "Component starts and stops server"
    (let [config {:host "0.1.2.3" :port 1234}

          handler (mkto/mock-fn)
          app-factory (mkto/on-call (mkto/mock-fn) [(Mockito/any)] handler)

          server-mock (mkto/mock java.io.Closeable)
          start-server-fn (mkto/on-call (mkto/mock-fn) [handler config] server-mock)]

      (with-redefs [aleph/start-server start-server-fn]

        (tu-comp/cycle-component

         [aleph (new-aleph app-factory config)]
         ((is (= (:server aleph) nil))
          (mkto/verify-fn start-server-fn [handler config] (mkto/never))
          (mkto/verify-> server-mock (.close) (mkto/never)))

         [started-aleph]
         ((is (= (:server started-aleph) server-mock))
          (mkto/verify-fn start-server-fn [handler config])
          (mkto/verify-> server-mock (.close) (mkto/never))
          (mkto/verify-fn app-factory [aleph]))

         [stopped-aleph]
         ((is (= (:stop-server stopped-aleph) nil))
          (mkto/verify-fn start-server-fn [handler config])
          (mkto/verify-> server-mock (.close))))))))

(deftest test-idempotent-aleph-component
  (testing "Component starts and stops server"
    (let [config {:host "0.1.2.3" :port 1234}

          handler (mkto/mock-fn)
          app-factory (mkto/on-call (mkto/mock-fn) [(Mockito/any)] handler)

          server-mock (mkto/mock java.io.Closeable)
          start-server-fn (mkto/on-call (mkto/mock-fn) [handler config] server-mock)]

      (with-redefs [aleph/start-server start-server-fn]
        (tu-comp/check-idempotence (new-aleph app-factory config))))))
