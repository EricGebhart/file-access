(ns file-access.core-test
  (:require [aws.sdk.s3 :as s3]
            [clj-cli-ext.core :as clie]
            [clojure.java.io :as io]
            [file-access.core :refer :all]
            [midje.sweet :refer :all]))

;; make sure you have the environment variables set.
;; If you have these set in your environment you should be able to use them to get to s3.
;; the test needs a bucket called "midge-bucket".
(def access-key (System/getenv "AWS_PUBLIC"))
(def secret-key (System/getenv "AWS_SECRET"))

;; Set up a commandline parser for the file options using clj-cli-ext.

(def file-options
  [["-f" "--file PATH" "File path to get file from."]
   ["-h" "--help"]])

(def sftp-options
  [["-P" "--port NUMBER" "access this port"
    :default 22
    :parse-fn #(Integer/parseInt %)
    :assert [#(< 0 % 0x10000) "%s is not a valid port number."]]
   ["-H" "--host HOST" "Connect to this hostname."
    :default-desc "localhost"
                                        ;:default (InetAddress/getByName "localhost")
                                        ;:parse-fn #(InetAddress/getByName %)
    ]
   ["-l" "--login ID" "Use this login id."]
   ["-p" "--password PASSWORD" "Use this password."]
   ["-h" "--help"]])

(def s3-options
  [["-b" "--bucket S3-Bucket" "Bucket to get file from."]
   ["-a" "--access-key ACCESS-KEY" "S3 access key."]
   ["-s" "--secret-key SECRET-KEY" "S3 secret key."]
   ["-f" "--file-key FILE-KEY" "File-key to retrieve from bucket."]
   ["-h" "--help"]])

(def github-file-options
  [["-r" "--repository Repository" "GitHub repository name." :required "Repository"]
   ["-u" "--user UserName" "GitHub user name." :required "UserName"]
   ["-p" "--path Path" "Path of file to get." :required "Path"]
   ["-b" "--branch Branch" "Repository branch to query." :default "master"]
   ["-a" "--auth Name:Password" "Authorization for repository."]
   ["-h" "--help"]])

(def file-sub-commands
  "define a map of sub commands, their options and a usage function."
  {:type :sub-command
   :parsers {:sftp  {:options sftp-options :description "Get a file from an sftp server."}
             :s3    {:options s3-options :description "Get an s3 file."}
             :github {:options github-file-options :description "Get a file from a github repository."}
             :file  {:options file-options :description "Get a local file."}}})

(defn parse-options [args & exceptions]
  (clie/do-parse args file-sub-commands {:pname "test-file-access"
                                         :version "1.0.0"
                                         :description "This is a test."
                                         :exception (or exceptions :exit)}))

(defn make-options [opts]
  (parse-options opts :none))


;; something to write to put in our file.
(def jabberwocky "Twas Brillig and the slithy toves did gyre and gimble in the wabe, all mimsy were the borogoves and the mome raths outgrabe.")

(defn build-s3-command
  "Build an options structure as if it had come from the commandline."
  [access-key secret-key bucket file]
  (make-options ["s3" "-a" access-key "-s" secret-key "-b" bucket "-f" file]))

(defn get-s3-map
  "Create a simple map of options"
  [access-key secret-key bucket file]
  {:access-key access-key :secret-key secret-key :bucket bucket :file-key file})

(defn create-s3-file
  "create a bucket and file on s3 and put jabberwocky in it."
  [s3-map]
  (let [{:keys [access-key secret-key bucket file-key]} s3-map
        cred (build-s3-credential access-key secret-key)]
    (s3/create-bucket cred bucket)
    (s3/put-object cred bucket file-key jabberwocky)
    (s3/update-object-acl cred bucket file-key (s3/grant :all-users :read))))

(defn cleanup-s3
  "delete the file and bucket from s3."
  [s3-map]
  (let [{:keys [access-key secret-key bucket file-key]} s3-map
        cred (build-s3-credential access-key secret-key)]
    (s3/delete-object cred bucket file-key)
    (s3/delete-bucket cred bucket)))

(facts "get a small local file"
       (slurp-file {:source :file :file "test/resources/test1.edn"})
       =>
       "{ :name \"TEST1\" :description \"TEST1 EDN\" }\n")

(facts "get a small local file"
       (slurp-file {:source :file :file "test/resources/nothere.edn"})
       => nil)

(facts "get a medium sized local file"
       (keys (read-string (slurp-file {:source :file :file "test/resources/lorem-ipsum.txt"})))
       =not=> nil)

#_(facts "get a small local file the harder way, with a reader."
         (reduce str "" (with-open [rdr (reader {:source :file :file "test/resources/test1.edn"})]
                          (doall (line-seq rdr))))
         =>
         "{ :name \"TEST1\" :description \"TEST1 EDN\" }")

(facts "get files from S3"

       (fact "create some credentials"
             (build-s3-credential "Public-key" "private-key")
             =>
             {:access-key "Public-key", :secret-key "private-key"})

       (facts "get an s3 file"

              (let [s3-map  (get-s3-map access-key
                                        secret-key
                                        "midje-bucket"
                                        "some-file")]

                (create-s3-file s3-map)

                (fact "get it with the get function"
                      (slurp-file (merge {:source :s3} s3-map)) => jabberwocky)

                (fact "read-file takes a flat map of options, including :source"
                      (keys (merge {:source :s3} s3-map)) => (contains [:access-key :secret-key
                                                                        :bucket :file-key :source] :in-any-order))

                (fact "let readfile dispatch the get"
                      (slurp-file (merge {:source :s3} s3-map)) => jabberwocky)

                (cleanup-s3 s3-map))

              (let [options (:main (build-s3-command
                                    access-key
                                    secret-key
                                    "midje-bucket"
                                    "some-file"))]

                (create-s3-file (:s3 options))

                (fact "commandline options are a nested map"
                      (keys options) => '(:s3)
                      (keys (:s3 options)) => (contains [:access-key :secret-key
                                                         :bucket :file-key] :in-any-order))

                (fact "construct a commandline and get a file."
                      (slurp-file-cmdline options) => jabberwocky)

                (cleanup-s3 (:s3 options)))))


(fact "get a small github file"
      (apply str
             (take 11
                   (slurp-file {:source :github :user "EricGebhart" :repository "emacs-setup" :path "README.md"})))
      =>
      "emacs-setup")

(fact "get a file from a private repo on github using the cli interface. Needs a good auth to work."
      (apply str (slurp-file-cmdline
                  (:main
                   (make-options
                    ["github"
                     "-u" "EricGebhart"
                     "-r" "file-access" ;; some private repo
                     "-p" "test/resources/test1.edn"
                     "-a" "EricGebhart:GitHubPWD"]))))
      =>
      "{ :name \"TEST1\" :description \"TEST1 EDN\" }\n")

(fact "get a file from github using the cli interface."
      (apply str
             (take 11
                   (slurp-file-cmdline
                    (:main (make-options
                            ["github"
                             "-u" "EricGebhart"
                             "-r" "emacs-setup"
                             "-p" "README.md"])))))
      =>
      "emacs-setup")
