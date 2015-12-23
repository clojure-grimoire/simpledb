(ns simpledb.core
  (:refer-clojure :exclude [get get-in])
  (:import java.util.concurrent.TimeUnit
           java.util.concurrent.Executors))

(defn put! [cfg k v]
  (let [*db* (:db cfg)]
    (swap! *db* assoc k v)
    [k v]))

(defn get [cfg k]
  (let [*db* (:db cfg)]
    (clojure.core/get @*db* k)))

(defn get-in [cfg k ks]
  (let [*db* (:db cfg)]
    (clojure.core/get-in (get k) ks)))

(defn remove! [cfg k]
  (let [*db* (:db cfg)]
    (swap! *db* dissoc k)
    k))

(defn update! [cfg k f & args]
  (let [*db* (:db cfg)]
    (assert *db* "Null database atom!")
    (clojure.core/get
     (swap! *db* #(assoc % k (apply f (clojure.core/get % k) args)))
     k)))

;; Backing file operations
;;------------------------------------------------------------------------------
(defn flush! [cfg]
  (let [*db*   (:db cfg)
        log-fn (:log-fn cfg)
        cur    @*db*]
    (log-fn (str "SimpleDB: Persisting " (count cur) " keys."))
    (spit (:file cfg) (pr-str cur))))

(defn read! [cfg]
  (let [*db*    (:db cfg)
        log-fn  (:log-fn cfg)
        content (try
                  (read-string (slurp (:file cfg)))
                  (catch Exception e
                    (log-fn "SimpleDB: Could not find the given file. Starting from scratch.")
                    {}))]
    (reset! *db* content)
    (let [not-empty? (complement empty?)]
      (when (not-empty? content)
        (log-fn "SimpleDB: " (count content) " keys are loaded.")
        true))))

(defn clear! [cfg]
  (let [db (:db cfg)]
    (reset! db {})
    (flush! cfg)))

(defn- compute-interval [[i t]]
  {:pre [(#{:seconds :minutes :hours :days} i)
         (number? t)
         (> t 0)]}
  (case i
    (:seconds) (long t)
    (:minutes) (recur [:seconds (* t 60)])
    (:hours)   (recur [:minutes (* 60 t)])
    (:days)    (recur [:hours (* 24 t)])))

;; Startup/shutdown operations
;;------------------------------------------------------------------------------
(defn start! [cfg]
  (let [timer    (:timer cfg)
        _        (assert timer)
        interval (:interval cfg)
        interval (compute-interval interval)]
    (assert (not (.isShutdown timer)) "Timer is shut down, db connection is closed!")
    (. timer (scheduleAtFixedRate (partial flush! cfg) interval interval (. TimeUnit SECONDS)))))

(defn stop! [cfg]
  (let [timer (:timer cfg)]
    (flush! cfg)
    (. timer (shutdown))
    (dissoc cfg :timer :db)))

(defn init!
  "Initializes a database.

  Accepts an optional map of settings
   - :file is the file to & from which the db will be written

   - :interval is the rate as a keyword, num pair w here the kw is one
     of #{:minutes :seconds :hours} and the number is how long in those
     to wait between writes.

   - :log-fn is a function of several arguments to be called when logging
     messages are generated, being a read, write start or shutdown.

   - :db is the atom to be used as the backing reference

   - :no-slurp disables the default initial slurp of the database
     into the backing atom."

  {:arglists '([] [{:keys [db file interval log-fn no-slurp]}])}
  [& [{:keys [file interval log-fn no-slurp db]
       :or   {file     "sdb.db"
              interval [:minutes 5]
              log-fn   (fn [& _] nil)
              no-slurp false}}]]
  (assert (vector? interval))
  (assert (#{:minutes :seconds :hours} (first interval)))
  (assert (number? (second interval)))
  (assert (fn? log-fn))
  
  (let [user-db  db
        timer    (. Executors newScheduledThreadPool 1)

        ;; Final configuration
        cfg      {:db       (or user-db (atom {}))
                  :file     file
                  :timer    timer
                  :interval interval
                  :log-fn   log-fn}]
    (when-not no-slurp
      (read! cfg))
    (start! cfg)
    cfg))
