(defproject dsl-crawler "0.1.0-SNAPSHOT"
  :description "Crawler collecting various stats from dsl.sk"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [clj-tagsoup "0.3.0"]
                 [clj-record "1.1.3"]
                 [postgresql/postgresql "9.1-901.jdbc4"]
                 [lambic "0.1.0"]]
  :main dsl-crawler.parser
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :test-paths ["test/clojure"])
