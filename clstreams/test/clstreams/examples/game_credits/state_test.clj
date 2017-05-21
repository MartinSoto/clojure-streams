(ns clstreams.examples.game-credits.state-test
  (:require [clojure.test :refer :all]
            [clstreams.examples.game-credits.state :as s :refer :all]))

(def has-10-credits-st {:credits 10})

(def add-5-credits-req {:type ::s/add-credits-requested
                        :credits 5})
(def use-5-credits-req {:type ::s/use-credits-requested
                        :credits 5})
(def use-15-credits-req {:type ::s/use-credits-requested
                         :credits 15})

(deftest state-update-test
  (testing "can create account"
    (let [{:keys [state errors]}
          (update-credits nil {:type ::s/create-account-requested})]
      (is (= (:credits state) 0))
      (is (nil? errors))))

  (testing "can add credits to a state"
    (let [{:keys [state errors]}
          (update-credits has-10-credits-st add-5-credits-req)]
      (is (= (:credits state) 15))
      (is (not errors))))

  (testing "can use credits if they're available"
    (let [{:keys [state errors]}
          (update-credits has-10-credits-st use-5-credits-req)]
      (is (= (:credits state) 5))
      (is (not errors))))

  (testing "trying to use more credits than available results in error"
    (let [{state :state {credits-error :credits} :errors}
          (update-credits has-10-credits-st use-15-credits-req)]
      (is (= state has-10-credits-st))
      (is (> (count credits-error) 0)))))
