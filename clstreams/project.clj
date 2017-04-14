(defproject clstreams "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/kafka-clients "0.10.1.0"]
                 [org.apache.kafka/kafka-streams "0.10.1.0"]]

  :main ^:skip-aot clstreams.core
  :target-path "target/%s"

  :profiles {:uberjar {:aot :all}})
