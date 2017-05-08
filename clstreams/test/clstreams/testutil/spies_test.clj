(ns clstreams.testutil.spies-test
  (:require [clojure.test :refer :all]
            [clstreams.testutil.spies :refer :all]))

(deftest test-spies
  (testing "Retrieve call list on a spy"
    (let [[f1 spy-f1] (spy :ze-ret)]
      (is (= (f1 43) :ze-ret))
      (is (= (calls spy-f1) [[43]]))

      (is (= (f1 "cuarenta" :y 'tres) :ze-ret))
      (is (= (calls spy-f1) [[43] ["cuarenta" :y 'tres]]))))
  (testing "Call spy with no parameters"
    (let [[f1 spy-f1] (spy 3)]
      (is (= (f1) 3))
      (is (= (calls spy-f1) [[]]))))
  (testing "Spy with default return value"
    (let [[f1 spy-f1] (spy)]
      (is (= (f1) nil)))))

(defn f1 [x] (* x 2))
(defn f2 [] (+ (f1 4) 21))

(deftest test-with-spy-redefs
  (testing "Replaces a function by a spy"
    (is (= (f2) 29))
    (with-spy-redefs [[f1 f1-spy] (spy 22)]
      (is (= (f2) 43))
      (is (= (calls f1-spy) [[4]])))))
