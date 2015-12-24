(ns simpledb.core
  (:refer-clojure :exclude [get get-in]))

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
        lock   (::lock cfg)
        cur    @*db*]
    (locking lock
      (log-fn (str "SimpleDB: Persisting " (count cur) " keys."))
      (spit (:file cfg) (pr-str cur)))))

(defn read! [cfg]
  (let [*db*    (:db cfg)
        log-fn  (:log-fn cfg)
        lock    (::lock cfg)
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

;; Startup/shutdown operations
;;------------------------------------------------------------------------------
(defn start! [cfg]
  (add-watch (:db cfg) ::watcher (fn [& _] (flush! cfg))))

(defn stop! [cfg]
  (remove-watch (:db cfg) ::watcher))

(defn init!
  "Initializes a database.

  Accepts an optional map of settings
   - :file is the file to & from which the db will be written

   - :log-fn is a function of several arguments to be called when logging
     messages are generated, being a read, write start or shutdown.

   - :db is the atom to be used as the backing reference

   - :no-slurp disables the default initial slurp of the database
     into the backing atom."

  {:arglists '([] [{:keys [db file interval log-fn no-slurp]}])}
  [& [{:keys [file interval log-fn no-slurp db]
       :or   {file     "sdb.db"
              log-fn   (fn [& _] nil)
              no-slurp false}}]]
  (assert (fn? log-fn))
  
  (let [user-db  db
        ;; Final configuration
        cfg      {:db       (or user-db (atom {}))
                  :file     file
                  :log-fn   log-fn
                  ::lock    (Object.)}]
    (when-not no-slurp
      (read! cfg))
    (start! cfg)
    cfg))
