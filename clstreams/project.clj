(defproject clstreams "0.1.0-SNAPSHOT"
  :description "Clojure Streams"
  :url "https://github.com/MartinSoto/clojure-streams"
  :license {:name "Apache 2.0 License"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}

  :dependencies [[com.stuartsierra/component "0.3.2"]
                 [org.apache.kafka/kafka-clients "0.10.2.1"]
                 [org.apache.kafka/kafka-clients "0.10.2.1" :classifier "test"]
                 [org.apache.kafka/kafka-streams "0.10.2.1"]
                 [org.apache.kafka/kafka-streams "0.10.2.1" :classifier "test"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.442"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-jdk14 "1.7.25"]
                 [ring/ring-json "0.4.0"]
                 [spootnik/signal "0.2.1"]
                 [aleph "0.4.3"]
                 [bidi "2.1.0"]]

  :main ^:skip-aot clstreams.core
  :target-path "target/%s"

  :profiles {:dev
             {:source-paths ["dev"]
              :dependencies [[org.clojure/java.classpath "0.2.3"]
                             [org.clojure/tools.namespace "0.3.0-alpha3"]]
              :main ^:skip-aot user}

             :uberjar
             {:aot :all}})
