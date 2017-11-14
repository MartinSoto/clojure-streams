(ns clstreams.topology-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [clstreams.topology :as prc]))

(deftest conform-predecessors-test
  (testing "s/conform converts predecessors to a map"
    (let [input [:op01 :op02]
          preds (s/conform ::prc/preds input)]
      (is (set? preds))
      (is (= preds #{:op02 :op01}))))
  (testing "invalid collections won't conform"
    (is (= (s/conform ::prc/preds []) ::s/invalid))
    (is (= (s/conform ::prc/preds ["op01" "op02"]) ::s/invalid))))


