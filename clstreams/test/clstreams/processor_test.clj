(ns clstreams.processor-test
  (:require [clojure.test :refer :all]
            [clstreams.processor :as prc]))

(deftest topological-order-test
  (testing "when an empty topology is provided then nil is returned"
    (is (= (prc/topological-order {}) nil)))
  (testing "when only sources are provided then all of them are returned"
    (let [top1 {:op01 {::prc/op ::prc/from
                       ::prc/topic :file-input}}
          top2 (assoc top1
                      :op02 {::prc/op ::prc/from
                             ::prc/topic :commands})]
      (is (= (set (prc/topological-order top1)) (set top1)))
      (is (= (set (prc/topological-order top2)) (set top2)))))
  (testing (str "when a linear operation sequence is provided then it "
                "is returned in the right order")
    (let [top1 [[:op01 {::prc/op ::prc/from
                        ::prc/topic :file-input}]
                [:op02 {::prc/op ::prc/flat-map-values
                        ::prc/src :op01
                        ::prc/fn identity}]
                [:op03 {::prc/op ::prc/map
                        ::prc/src :op02
                        ::prc/fn identity}]
                [:op04 {::prc/op ::prc/group-by-key
                        ::prc/src :op03}]
                [:op05 {::prc/op ::prc/count
                        ::prc/src :op04
                        ::prc/store "Counts"}]
                [:op06 {::prc/op ::prc/to
                        ::prc/src :op05
                        ::prc/topic :word-counts}]]]
      (is (= (prc/topological-order (into {} top1)) (seq top1))))))
