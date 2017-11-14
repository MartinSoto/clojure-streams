(ns clstreams.traverse
  (:require [flatland.ordered.map :refer [ordered-map]]
            [flatland.ordered.set :refer [ordered-set]]
            [clstreams.topology :as tp]))

(defn- order-from-node [ordered pending visiting node-id]
  (cond
    (ordered node-id) [ordered pending]
    (visiting node-id) [::tp/cycle (reverse (drop-while #(not= % node-id) visiting))]
    :else
    (let [{node-pred-ids ::tp/preds :as node-value} (pending node-id)
          visiting (conj visiting node-id)
          pending (dissoc pending node-id)]
      (let [[ordered pending :as result]
            (loop [ordered ordered
                   pending pending
                   [pred-id & pred-ids] node-pred-ids]
              (if-not pred-id
                [ordered pending]
                (let [[ordered pending :as result]
                      (order-from-node ordered pending visiting pred-id)]
                  (if (= ordered ::tp/cycle)
                    result
                    (recur ordered pending pred-ids)))))]
        (if (= ordered ::tp/cycle)
          result
          [(assoc ordered node-id node-value) pending])))))

(defn order-nodes
  ([nodes]
   (order-nodes (ordered-map) nodes))
  ([ordered pending]
   (let [[[node-id _] & _] (seq pending)]
     (if-not node-id
       ordered
       (let [[ordered pending] (order-from-node ordered pending (ordered-set) node-id)]
         (if (= ordered ::tp/cycle)
           {::tp/cycle pending}
           (recur ordered pending)))))))
