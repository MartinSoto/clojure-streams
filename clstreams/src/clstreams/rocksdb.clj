(ns clstreams.rocksdb
  (:import org.rocksdb.Options
           org.rocksdb.RocksDB)
  (:require [clojure.java.io :as io]))

(defn open-db [dir]
  (let [options (Options.)]
    (.setCreateIfMissing options true)
    (io/make-parents dir)
    (RocksDB/open options dir)))
