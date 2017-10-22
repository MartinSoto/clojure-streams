(ns clstreams.examples.count-words-prc.topology-test
  (:require [clstreams.examples.count-words-prc.topology :as sut]
            [clojure.test :refer :all]
            [clstreams.testutil.kstreams
             :refer
             [default-props string-deserializer string-serializer]])
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.streams.StreamsConfig
           org.apache.kafka.test.ProcessorTopologyTestDriver
           org.apache.kafka.streams.processor.Processor
           org.apache.kafka.streams.processor.TopologyBuilder
           org.apache.kafka.streams.processor.TopologyBuilder))


(defn test-driver
  ([^KStreamBuilder builder ^StreamsConfig props]
   (ProcessorTopologyTestDriver. props builder))
  ([^KStreamBuilder builder]
   (test-driver builder (default-props))))

(defn process [^ProcessorTopologyTestDriver driver topic msgs]
  (doseq [[key value] msgs]
    (.process driver topic key value string-serializer string-serializer)))

(defn read-output [^ProcessorTopologyTestDriver driver topic]
  (if-let [record (.readOutput driver topic
                               string-deserializer string-deserializer)]
    (lazy-seq (cons [(.key record) (.value record)] (read-output driver topic)))))

(defn through-kstreams-topology
  ([^KStreamBuilder builder msgs]
   (through-kstreams-topology builder msgs "input" "output"))
  ([^KStreamBuilder builder msgs input-topic output-topic]
   (with-open [driver (test-driver builder)]
     (process driver input-topic msgs)
     (read-output driver output-topic))))


(defn single-processor-topology [^Processor processor]
  (let [builder (TopologyBuilder.)]
    (.addSource builder "src1" (into-array String '("input")))
    (.addProcessor builder "prc1" processor (into-array String '("src1")))
    (.addSink builder "snk1" "output" (into-array String '("prc1")))
    builder))

(defn through-kstreams-processor [^Processor processor msgs]
  (through-kstreams-topology (single-processor-topology processor) msgs))


(deftest test-transducing-processor
  (let [data [["a" "1"] ["b" "2"] ["c" "3"]]]
    (testing "can perform arbitrary reductions on its input"
      (let [state (atom 0)
            reducer (completing (fn [st val] (swap! st + val) st))]
        (through-kstreams-processor
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
      (is (= (through-kstreams-processor
              (sut/transducing-processor
               (constantly sut/forward-reducer)
               identity)
              data)
             data)))
    (testing "passes the same processing context to both init functions"
      (let [ctx1 (atom nil)
            ctx2 (atom nil)]
        (through-kstreams-processor
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
    (is (= (through-kstreams-processor
            (sut/key-value-processor
             identity)
            data)
           data))
    (is (= (through-kstreams-processor
            (sut/key-value-processor
             (map (fn [[key value]] [value key])))
            data)
           [["1" "a"] ["2" "b"] ["3" "c"]]))
    (is (= (through-kstreams-processor
            (sut/key-value-processor
             (map (fn [[key value]] [value key]))
             (filter (fn [[key value]] (odd? (Integer. key)))))
            data)
           [["1" "a"] ["3" "c"]]))))


(deftest test-map-store
  (let [st-name "zeName"
        make-store (fn [] (.get (sut/map-store st-name)))]
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
      (is (= (sut/store-get store :a) 84)))))


(deftest test-xform-values
  (let [data [[:a 1] [:b 2] [:c 3]]]
    (testing "applied to identity results in identity transform"
      (is (= (transduce (sut/xform-values identity) conj data) data)))
    (testing "map affects only keys"
      (is (= (transduce (sut/xform-values (map inc)) conj data)
             [[:a 2] [:b 3] [:c 4]])))
    (testing "composes several transforms"
      (is (= (transduce (sut/xform-values (map inc) (map str)) conj data)
             [[:a "2"] [:b "3"] [:c "4"]])))
    (testing "can produce zero or several results per key"
      (is (= (transduce (sut/xform-values (map dec) (mapcat range)) conj data)
             [[:b 0] [:c 0] [:c 1]])))))


(deftest test-word-count-topology
  (let [builder (sut/build-word-count-topology)
        msgs [["" "these  are"]
              ["" " some words"]]
        expected [["these" "these"]
                  ["are" "are"]
                  ["some" "some"]
                  ["words" "words"]]]
    (is (= (through-kstreams-topology builder msgs) expected))))

