(defproject clstreams "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.namespace "0.3.0-alpha3"]

                 [org.apache.kafka/kafka-clients "0.10.2.1"]
                 [org.apache.kafka/kafka-clients "0.10.2.1" :classifier "test"]
                 [org.apache.kafka/kafka-streams "0.10.2.1"]
                 [org.apache.kafka/kafka-streams "0.10.2.1" :classifier "test"]
                 [org.slf4j/slf4j-jdk14 "1.7.25"]

                 [com.stuartsierra/component "0.3.2"]
                 [spootnik/signal "0.2.1"]]

  :main ^:skip-aot clstreams.core
  :target-path "target/%s"

  :profiles {:uberjar {:aot :all}})
