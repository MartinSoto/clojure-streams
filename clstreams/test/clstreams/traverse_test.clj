(ns clstreams.traverse-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clstreams.topology :as tp]
            [clstreams.traverse :as sut]))

(defn verify-topological-order
  ([ordered] (verify-topological-order #{} ordered))
  ([seen-ids [node & remaining-ordered]]
   (if-let [[node-id {preds ::tp/preds}] node]
     (if-let [not-preceding (seq (remove seen-ids preds))]
       (format "Node %s not preceded by expected predecesor(s) %s"
               node-id (str/join ", " not-preceding))
       (recur (conj seen-ids node-id) remaining-ordered))
     nil)))

(deftest verify-topological-order-test
  (is (nil? (verify-topological-order
             [[:n1 {}]
              [:n2 {::tp/preds [:n1]}]])))
  (is (= (verify-topological-order
          [[:n1 {}]
           [:n2 {::tp/preds [:n3 :n1 :n4]}]])
         "Node :n2 not preceded by expected predecesor(s) :n3, :n4")))


(defn verify-cycle [nodes cycle]
  (let [broken-links
        (->> (map vector cycle (concat (rest cycle) [(first cycle)]))
             (remove (fn [[pred succ]] ((into #{} (::tp/preds (nodes succ))) pred))))]
    (if-let [[pred succ] (first (seq broken-links))]
      (format "Node %s is not a succesor of %s in cycle %s" succ pred (into [] cycle)))))

(deftest verify-cycle-test
  (is (nil? (verify-cycle {:op01 {::tp/preds [:op02]}
                           :op02 {::tp/preds [:op01]}}
                          [:op01 :op02])))
  (is (= (verify-cycle {:op01 {::tp/preds [:op02]}
                        :op02 {::tp/preds []}}
                       [:op01 :op02])
         "Node :op02 is not a succesor of :op01 in cycle [:op01 :op02]")))


(defn verify-order-nodes [nodes]
  (let [ordered (sut/order-nodes nodes)
        {cycle ::tp/cycle} ordered]
    (if cycle
      (or
       (if (empty? cycle) "order-nodes returned an empty cycle")
       (verify-cycle nodes cycle))
      (or
       (verify-topological-order (seq ordered))
       (if (not= nodes ordered) "ordered map doesn't contain same nodes as original")))))

(defn check-order-nodes [nodes]
  (let [msg (verify-order-nodes nodes)] (is (not msg) msg)))

(deftest order-nodes-test
  (testing "empty topology"
    (check-order-nodes {}))
  (testing "only sources"
    (let [top1 {:op01 {}}
          top2 (assoc top1 :op02 {})]
      (check-order-nodes top1)
      (check-order-nodes top2)))
  (testing "linear processor sequence"
    (check-order-nodes {:op01 {}
                        :op02 {::tp/preds [:op01]}
                        :op03 {::tp/preds [:op02]}
                        :op04 {::tp/preds [:op03]}
                        :op05 {::tp/preds [:op04]}
                        :op06 {::tp/preds [:op05]}}))
  (testing "two separate strands"
    (check-order-nodes {:op01 {}
                        :op02 {::tp/preds [:op01]}
                        :op03 {::tp/preds [:op02]}
                        :op04 {}
                        :op05 {::tp/preds [:op04]}
                        :op06 {::tp/preds [:op05]}})))

(deftest order-nodes-with-cycle-test
  (testing "one node pointing to itself"
    (check-order-nodes {:op01 {::tp/preds [:op01]}}))
  (testing "two nodes in a cycle"
    (check-order-nodes {:op01 {::tp/preds [:op02]}
                        :op02 {::tp/preds [:op01]}}))
  (testing "larger cycle"
    (check-order-nodes {:op01 {::tp/preds [:op03]}
                        :op02 {::tp/preds [:op01]}
                        :op03 {::tp/preds [:op02]}}))
  (testing "two nodes in a cycle in a larger graph"
    (check-order-nodes {:op01 {::tp/preds [:op02]}
                        :op02 {::tp/preds [:op01]}
                        :op03 {::tp/preds []}})))

(defn- label [n] (keyword (format "op%02d" n)))

(defn gen-graph [size]
  (into {}
        (for [i (range size)
              :let [preds (gen/generate (gen/vector-distinct (gen/elements (range size))))]]
          [(label i) {::tp/preds (into [] (map label preds))}])))

(defn gen-dag [size]
  (into {}
        (for [i (range size)
              :let [preds (if (= i 0)
                            []
                            (gen/generate (gen/vector-distinct (gen/elements (range i)))))]]
          [(label i) {::tp/preds (into [] (map label preds))}])))

(defspec order-nodes-is-topological-prop
  50
  (prop/for-all [nodes (gen/fmap gen-dag gen/int)]
                (let [ordered (sut/order-nodes nodes)
                      msg (verify-topological-order (seq ordered))]
                  (and (= nodes ordered)
                       (nil? msg)))))

(defspec order-nodes-works-or-checks-for-cycles
  50
  (prop/for-all [nodes (gen/fmap gen-graph gen/int)]
                (let [msg (verify-order-nodes nodes)]
                  (and (nil? msg)))))
