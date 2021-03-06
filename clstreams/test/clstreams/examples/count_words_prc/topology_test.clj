(ns clstreams.examples.count-words-prc.topology-test
  (:require [clojure.test :refer :all]
            [clstreams.examples.count-words-prc.topology :as sut]
            [clstreams.testutil.topology-driver :as drv]))

(deftest test-word-count-topology
  (let [builder (sut/build-word-count-topology)
        msgs [["" "these  are"]
              ["" " some words"]]
        expected [["these" "these"]
                  ["are" "are"]
                  ["some" "some"]
                  ["words" "words"]]]
    (is (= (drv/through-kstreams-topology sut/count-words-landscape
                                      builder :input :words msgs) expected)))
  (let [builder (sut/build-word-count-topology)
        msgs [["" "these  are some words "]
              ["" "These are some more"]]
        expected {"these" "2"
                  "are" "2"
                  "some" "2"
                  "words" "1"
                  "more" "1"}]
    (is (= (into {} (drv/through-kstreams-topology sut/count-words-landscape
                                               builder :input :output msgs))
           expected))))

