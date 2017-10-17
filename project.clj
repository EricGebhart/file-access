(defproject file-access "0.1.0"
  :description "A library that enables easy reading of files from the local filesystem, SFTP, S3, and github"
  :url "http://github.com/EricGebhart/file-access"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [midje "1.8.3"]
                 [clj-ssh "0.5.14"]
                 [com.taoensso/timbre "4.10.0"]
                 [clj-http "3.7.0"]
                 [clojure.joda-time "0.7.0"]
                 [clj-aws-s3 "0.3.10" :exclusions [joda-time]]
                 [tentacles "0.5.1"
                  :exclusions [org.clojure/clojure com.fasterxml.jackson.core/jackson-core]]
                 [clj-cli-ext "0.1.1"]])
