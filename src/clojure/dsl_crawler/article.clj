(ns dsl-crawler.article
  (:require clj-record.boot))

(def db {:classname "org.postgresql.Driver"
           :subprotocol "postgresql"
           :subname "//localhost:5432/dsl"
           :user "postgres"
           :password ""})

(clj-record.core/init-model)
