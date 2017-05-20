(ns clstreams.kstreams.serdes-test
  (:require [clstreams.kstreams.serdes :as sut]
            [clojure.test :refer :all]))

(deftest test-edn-serializer
  (testing "Default config works with basic types"
    (let [ser (sut/edn-serializer)]
      (is (= (seq (.serialize ser "topic1" 3)) (seq (.getBytes "3" "UTF8"))))
      (is (= (seq (.serialize ser "topic1" [1 2])) (seq (.getBytes "[1 2]" "UTF8")))))))

(deftest test-edn-deserializer
  (testing "Default config works with basic types"
    (let [des (sut/edn-deserializer)]
      (is (= (.deserialize des "topic1" (.getBytes "3" "UTF8")) 3))
      (is (= (.deserialize des "topic1" (.getBytes "[1 2]" "UTF8")) [1 2])))))

(deftest test-edn-serialization-roundtrip
  (let [ser (sut/edn-serializer)
        des (sut/edn-deserializer)
        rtrip (fn [data]
                (->> data
                     (.serialize ser "t1")
                     (.deserialize des "t1")))]
    (is (= (rtrip 42) 42) "Works with integers")
    (is (= (rtrip "abc") "abc") "Works with strings")
    (is (= (rtrip [1 "yeah" :no]) [1 "yeah" :no]) "Works with vectors")
    (is (= (rtrip {:a "one" :b 2}) {:a "one" :b 2})) "Works with maps"))
