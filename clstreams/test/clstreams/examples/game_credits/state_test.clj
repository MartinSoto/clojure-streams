(ns clstreams.examples.game-credits.state-test
  (:require [clojure.test :refer :all]
            [clstreams.examples.game-credits.state :as sut]))

(def has-10-credits-st {:balance 10})

(def add-5-credits-req {:type ::sut/add-credits-requested
                        :credits 5})
(def use-5-credits-req {:type ::sut/use-credits-requested
                        :credits 5})
(def use-15-credits-req {:type ::sut/use-credits-requested
                         :credits 15})

(deftest state-update-test
  (testing "can create account"
    (let [{:keys [type balance credits errors]}
          (sut/update-credits nil {:type ::sut/create-account-requested})]
      (is (= type ::sut/account-created))
      (is (= balance 0))
      (is (nil? credits))
      (is (nil? errors))))

  (testing "can add credits to the balance"
    (let [{:keys [type balance credits errors]}
          (sut/update-credits has-10-credits-st add-5-credits-req)]
      (is (= type ::sut/credits-added))
      (is (= balance 15))
      (is (= credits) 5)
      (is (nil? errors))))

  (testing "can use credits if they're available"
    (let [{:keys [type balance credits errors]}
          (sut/update-credits has-10-credits-st use-5-credits-req)]
      (is (= type ::sut/credits-used))
      (is (= balance 5))
      (is (= credits) 5)
      (is (nil? errors))))

  (testing "trying to use more credits than available results in error"
    (let [{:keys [type balance credits errors]}
          (sut/update-credits has-10-credits-st use-15-credits-req)]
      (is (= type ::sut/insufficient-credits-error))
      (is (nil? balance))
      (is (nil? credits))
      (is (some? errors)))))
