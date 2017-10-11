(ns clstreams.rocksdb-test
  (:require [clstreams.rocksdb :as sut]
            [clojure.test :refer :all]))

(defn delete-recursively [fname]
  (let [file (clojure.java.io/file fname)]
    (if (.exists (clojure.java.io/as-file file))
      (doseq [f (reverse (file-seq file))]
        (clojure.java.io/delete-file f)))))

(deftest rocksdb-open-test
  (testing "can open a rocksdb DB"
    (delete-recursively "/tmp/ddeDB")
    (with-open [db (sut/create-db "/tmp/ddeDB/somedb")]
      (is (instance? Object db)))
    (delete-recursively "/tmp/ddeDB")))
