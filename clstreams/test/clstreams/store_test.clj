(ns clstreams.store-test
  (:require [clojure.test :refer :all]
            [clstreams.store :as sut]
            [clstreams.testutil.topology-driver :as drv]))

(def st-name "zeName")
(def make-store (fn [] (.get (sut/map-store st-name))))

(deftest test-map-store
  (is (= (.name (sut/map-store st-name)) st-name))
  (is (= (.name (make-store)) st-name))
  (is (= (into #{} (sut/store-keys (make-store))) #{}))
  (let [store (make-store)]
    (is (= (sut/store-assoc! store :a 42) store))
    (is (= (sut/store-get store :a) 42))
    (is (= (into #{} (sut/store-keys store)) #{:a}))
    (sut/store-assoc! store :b 75)
    (is (= (into #{} (sut/store-keys store)) #{:a :b}))
    (is (= (sut/store-dissoc! store :a) store))
    (is (= (into #{} (sut/store-keys store)) #{:b}))
    (is (= (sut/store-get store :b) 75)))
  (let [store (make-store)]
    (sut/store-assoc! store :a 42)
    (is (= (sut/store-update! store :a (fn [v] (* v 2))) store))
    (is (= (sut/store-get store :a) 84))))

(deftest test-lookup-processor
  (let [store (-> (make-store)
                  (sut/store-assoc! "a" "AA")
                  (sut/store-assoc! "b" "BB")
                  (sut/store-assoc! "c" "CC"))
        data [["a" "1"] ["b" "2"] ["c" "3"]]]
    (is (= (drv/through-kstreams-processor
            (sut/lookup-processor store)
            data)
           [["a" ["1" "AA"]] ["b" ["2" "BB"]] ["c" ["3" "CC"]]]))))
