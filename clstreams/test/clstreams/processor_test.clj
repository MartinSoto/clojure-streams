(ns clstreams.processor-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clstreams.processor :as prc]
            [flatland.ordered.map :refer [ordered-map]]))

(defn verify-topological-order
  ([ordered] (verify-topological-order #{} ordered))
  ([seen-ids [node & remaining-ordered]]
   (if-let [[node-id {preds ::prc/preds}] node]
     (if-let [not-preceding (seq (remove seen-ids preds))]
       (format "Node \"%s\" not preceded by expected predecesor(s) \"%s\""
               node-id (str/join "\", \"" not-preceding))
       (recur (conj seen-ids node-id) remaining-ordered))
     nil)))

(deftest verify-topological-order-test
  (is (nil? (verify-topological-order
             [[:n1 {}]
              [:n2 {::prc/preds [:n1]}]])))
  (is (= (verify-topological-order
          [[:n1 {}]
           [:n2 {::prc/preds [:n3 :n1 :n4]}]])
         "Node \":n2\" not preceded by expected predecesor(s) \":n3\", \":n4\"")))

(defn check-order-nodes [nodes]
  (let [ordered (prc/order-nodes nodes)
        msg (verify-topological-order (seq ordered))]
    (is (= nodes ordered) "same nodes are present in the ordered map")
    (if msg (is false msg))))

(deftest order-nodes-test
  (testing "empty topology"
    (check-order-nodes {}))
  (testing "only sources"
    (let [top1 {:op01 {}}
          top2 (assoc top1 :op02 {})]
      (check-order-nodes top1)
      (check-order-nodes top2)))
  (testing (str "linear processor sequence")
    (let [top1 {:op01 {}
                :op02 {::prc/preds [:op01]}
                :op03 {::prc/preds [:op02]}
                :op04 {::prc/preds [:op03]}
                :op05 {::prc/preds [:op04]}
                :op06 {::prc/preds [:op05]}}]
      (check-order-nodes top1))))
