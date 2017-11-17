(ns clstreams.processor-test
  (:require [clojure.test :refer :all]
            [clstreams.processor :as sut]
            [clstreams.testutil.topology-driver :as drv]))

(deftest test-transducing-processor
  (let [data [["a" "1"] ["b" "2"] ["c" "3"]]]
    (testing "can perform arbitrary reductions on its input"
      (let [state (atom 0)
            reducer (completing (fn [st val] (swap! st + val) st))]
        (drv/through-kstreams-processor
         (sut/transducing-processor
          (fn [context]
            ((comp
              (map #(nth % 1))
              (map #(Integer/parseInt %)))
             reducer))
          (fn [context] state))
         data)
        (is (= @state 6))))
    (testing "passes the processing context to the init function"
      (is (= (drv/through-kstreams-processor
              (sut/transducing-processor
               (constantly sut/forward-reducer)
               identity)
              data)
             data)))
    (testing "passes the same processing context to both init functions"
      (let [ctx1 (atom nil)
            ctx2 (atom nil)]
        (drv/through-kstreams-processor
         (sut/transducing-processor
          (fn [context]
            (reset! ctx1 context)
            sut/forward-reducer)
          (fn [context]
            (reset! ctx2 context)
            context))
         data)
        (is (not (nil? ctx1)))
        (is (= @ctx1 @ctx2))))))


(deftest test-key-value-processor
  (let [data [["a" "1"] ["b" "2"] ["c" "3"]]]
    (is (= (drv/through-kstreams-processor
            (sut/key-value-processor
             identity)
            data)
           data))
    (is (= (drv/through-kstreams-processor
            (sut/key-value-processor
             (map (fn [[key value]] [value key])))
            data)
           [["1" "a"] ["2" "b"] ["3" "c"]]))
    (is (= (drv/through-kstreams-processor
            (sut/key-value-processor
             (map (fn [[key value]] [value key]))
             (filter (fn [[key value]] (odd? (Integer. key)))))
            data)
           [["1" "a"] ["3" "c"]]))
    (is (= (drv/through-kstreams-processor
            (sut/key-value-processor
             (mapcat (fn [[key value]] [[value (str key "x")] [value (str key "y")]])))
            data)
           [["1" "ax"] ["1" "ay"] ["2" "bx"] ["2" "by"] ["3" "cx"] ["3" "cy"]]))))


(deftest test-value-processor
  (let [data [["a" "1"] ["b" "2"] ["c" "3"]]]
    (is (= (drv/through-kstreams-processor
            (sut/value-processor
             identity)
            data)
           data))
    (is (= (drv/through-kstreams-processor
            (sut/value-processor
             (map (comp str inc #(Integer. %))))
            data)
           [["a" "2"] ["b" "3"] ["c" "4"]]))
    (is (= (drv/through-kstreams-processor
            (sut/value-processor
             (map (comp inc #(Integer. %)))
             (filter even?)
             (map str))
            data)
           [["a" "2"] ["c" "4"]]))
    (is (= (drv/through-kstreams-processor
            (sut/value-processor
             (mapcat (fn [v] [(str v "x") (str v "y")])))
            data)
           [["a" "1x"] ["a" "1y"] ["b" "2x"] ["b" "2y"] ["c" "3x"] ["c" "3y"]]))))

