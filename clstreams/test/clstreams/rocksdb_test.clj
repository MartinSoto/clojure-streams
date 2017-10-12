(ns clstreams.rocksdb-test
  (:require [clstreams.rocksdb :as sut]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(def db-dir "/tmp/ddeDB/somedb")
(def db-dir-file (io/file db-dir))

(defn delete-recursively [file]
  (if (.exists file)
    (doseq [f (reverse (file-seq file))]
      (io/delete-file f))))

(defn clean-db-fixture [run-test]
  (let [base-dir-file (.getParentFile db-dir-file)]
    (delete-recursively base-dir-file)
    (run-test)
    (delete-recursively base-dir-file)))

(use-fixtures :each clean-db-fixture)

(deftest rocksdb-open-test
  (testing "can open a rocksdb DB"
    (with-open [db (sut/create-db db-dir)]
      (is (instance? Object db))))
  (testing "can write and read from a DB"
    (with-open [db (sut/create-db db-dir)]
      (let [key "zeKey"
            value "ze value"]
        (.put db (.getBytes key) (.getBytes value))
        (is (= (String. (.get db (.getBytes key))) value)))))
  (testing "can read from an existing DB"
    (let [key "zeKey"
          value "ze value"]
      (with-open [db (sut/create-db db-dir)]
        (.put db (.getBytes key) (.getBytes value)))
      (with-open [db (sut/create-db db-dir)]
        (is (= (String. (.get db (.getBytes key))) value))))))
