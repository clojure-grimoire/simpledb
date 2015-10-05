(ns simpledb.test.core
  (:refer-clojure :exclude [get get-in])  
  (:use [simpledb.core])
  (:use [clojure.test]))

(def cfg (init!))

(start! cfg)

(deftest write-test 
  (let [t (System/currentTimeMillis)]
    (loop [iter 0]
      (if (>= (- (System/currentTimeMillis) t) 1000)
        (println iter)
        (do
          (put! cfg iter iter)
          (recur (inc iter)))))))

(deftest read-test 
  (let [t (System/currentTimeMillis)]
    (loop [iter 0]
      (if (>= (- (System/currentTimeMillis) t) 1000)
        (println iter)
        (do
          (get cfg iter)
          (recur (inc iter)))))))


