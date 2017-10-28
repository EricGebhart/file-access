(ns org.eag.file-access.core
  (:require [aws.sdk.s3 :as s3]
            [clj-ssh.ssh :as ssh]
            [tentacles.repos :as github]
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [clojure.java.io :as io]))

(timbre/refer-timbre)

(defn build-s3-credential
  "build an s3 credential"
  [access-key secret-key]
  {:access-key access-key :secret-key secret-key})

;; reader creates a reader for for local files, s3, github, and sftp.
;; If you don't care about laziness just use slurp file below.
;; otherwise you'll want to use the reader with-open or in some other
;; way to take advantage of laziness.
(defn reader-dispatch [x] (:source x))

(defmulti reader reader-dispatch)

(defmethod reader :file [{:keys [filename]}]
  (io/reader filename))

(defmethod reader :local [{:keys [filename]}]
  (io/reader filename))

(defmethod reader :default [{:keys [filename]}]
  (io/reader filename))

(defmethod reader :github
  [{:keys [user repository path branch auth] :or {branch "master"}}]
  (let [options (merge {:str? nil :ref branch}
                       (when auth {:auth auth}))
        content (github/contents user repository path options)
        status (:status content)
        message (:message (:body content))]
    (if (nil? status)
      (io/reader (:content content))
      (throw (Exception. (str "Github: " status " " message))))))

(defmethod reader :s3
  [{:keys [file-key bucket access-key secret-key]}]
  (let [cred (build-s3-credential access-key secret-key)]
    (io/reader (:content (s3/get-object cred bucket file-key)))))

(defmethod reader :sftp
  [{:keys [filename host private-key]}]

  (let [agent (ssh/ssh-agent {})]
    ;;[agent (ssh/ssh-agent {:use-system-ssh-agent false})]
    ;;(ssh/add-identity agent {:private-key-path "/user/name/.ssh/id_rsa"})
    (let [session (ssh/session agent host {:strict-host-key-checking :no})]
      (ssh/with-connection session
        (let [channel (ssh/ssh-sftp session)]
          (ssh/with-channel-connection channel
            (io/reader (ssh/sftp channel {} :get filename))))))))

;; {:name "Something-from-S3"
;;  :filename "local or sftp filename."
;;  :type "csv"
;;  :source :s3  ;; :s3, :local or :filename, :sftp, :github

;;  ;; S3, SFTP
;;  :file-key "S3 file key"
;;  :bucket "some.s3.bucket"
;;  :access-key "S3 access key"
;;  :private-key "S3 or sftp private key"
;;  :host "some sftp host"

;;  ;; Github
;;  :user "somegithub user."
;;  :repo "github repository"
;;  :branch "repository branch if not master"
;;  :path  "path to file"
;;  :auth  "authorization if not public."
;;  }

;;; Use only for smaller in memory files.
;;; See the CSV importer for a better method using channels !!!!!!!!
;;; Use the readers above, but with a loop which places the data on a channel.

(defn slurp-file
  "slurp a file using one of the readers above. Identical to core.slurp with our own readers."
  [options]
  (info "File Access:")
  (info options)
  (try
    (let [sb (StringBuilder.)]
      (with-open [rdr (reader options)]
        (loop [c (.read rdr)]
          (if (neg? c)
            (str sb)
            (do
              (.append sb (char c))
              (recur (.read rdr)))))))
    (catch Exception e
      (let  [{:keys [type status message cli]} (ex-data e)
             message (str "Read file failed: " options "  " (.getMessage e) "\n")]
        ;;(println message)
        (error message))
      nil)))



(defn slurp-file-cmdline
  "read a file from a source - based on the options as built by the commandline interface.
  ie. {:file {:filename <filename>}} --> {:source :file :filename <filename>}
  ie. {:github {:path <filename> ... }} --> {:source :github :path <filename> ...} "
  [options]
  (let [transport (first (keys options))
        options (merge {:source transport} (transport options))]
    (slurp-file options)))

;;;;;;;;;;;;;;;; EXAMPLES ;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (with-open [rdr (clojure.java.io/reader "/etc/passwd")]
;;   (count (line-seq rdr)))


;; (defn lazy-file-lines
;;   "open a (probably large) file and make it a available as a lazy seq of lines"
;;   [filename]
;;   (letfn [(helper [rdr]
;;             (lazy-seq
;;              (if-let [line (.readLine rdr)]
;;                (cons line (helper rdr))
;;                (do (.close rdr) nil))))]
;;     (helper (clojure.java.io/reader filename))))


;;;;;;;Using channels

;; (ns user
;;   (:require [clojure.core.async :as async :refer :all
;;              :exclude [map into reduce merge partition partition-by take]]))

;; (defn read-dir [dir]
;;   (let [directory (clojure.java.io/file dir)
;;         files (filter #(.isFile %) (file-seq directory))
;;         ch (chan)]
;;     (go
;;       (doseq [file files]
;;         (with-open [rdr (clojure.java.io/reader file)]
;;           (doseq [line (line-seq rdr)]
;;             (>! ch line))))
;;       (close! ch))
;;     ch))
;; invoke:

;; (def aa "D:\\Users\\input")
;; (let [ch (read-dir aa)]
;;   (loop []
;;     (when-let [line (<!! ch )]
;;       (println line)
;;       (recur))))


;;;;;;;;;;;;;;;;;;;;;;;;;

;;One suggestion:

;; Use line-seq to get a lazy sequence of lines,
;; use map to parse each line,
;; (so far this matches what you are doing)

;; use partition-all to partition your lazy sequence of parsed lines into batches, and then
;; use insert-batch with doseq to write each batch to the database.
;; And an example:

;; (->> (line-seq reader)
;;      (map parse-line)
;;      (partition-all 1000)
;;      (#(doseq [batch %]
;;          (insert-batch batch))))


;;;;;;;;;;;;;;;;;;;;;;;  core.async

;; (defn process-file [ch file]
;;   (async/thread
;;     (with-open [input (io/reader file)]
;;       (doseq [line (line-seq input)]
;;         (async/>!! ch line)))))

;; (defn parse [line]
;;   (str "Parsed: " line)) ; change it to do whatever you want

;; (defn mapping [ch]
;;   (async/map parse [ch]))

;; (defn start []
;;   (let [events (mapping
;;                 (async/chan))]
;;     (process-file events "10_events.json")
;;     (async/go-loop []
;;       (let [v (async/<! events)]
;;         (println v)
;;         (recur)))))
