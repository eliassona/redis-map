(ns redismap.core
  (:require [clojure.data.json :as json])
  (:import [[redis.clients.jedis Jedis]]
           [clojure.lang MapEntry]
           [java.util AbstractMap$SimpleEntry])
  )

(defmacro dbg [body]
  `(let [x# ~body]
     (println "dbg:" '~body "=" x#)
     x#))


(defprotocol ISerializer
  (serialize [this data])
  (deserialize [this data]))

(deftype JsonSerializer []
  ISerializer
  (serialize [this data] (json/json-str data))
  (deserialize [this data] (json/read-str data)))
  
(defn key-of 
  ([prefix k]
    (str prefix k))
  ([prefixed-key]
   (second (clojure.string/split prefixed-key #":")))) 
   


(defn entries-of [jedis prefix serializer assocMap withouts]
  (filter
    (fn [e]
      (not (contains? withouts (.getKey e))))
      (map 
        (fn [k] 
          (AbstractMap$SimpleEntry. 
            (key-of k)
            (if (contains? assocMap k)
              (get assocMap k)
              (.deserialize serializer (.get jedis k)))))
               (.keys jedis (key-of prefix "*")))))


(deftype RedisPersistentMap [jedis prefix serializer withouts assocMap]
  clojure.lang.IFn
  (invoke [this k]
    (deserialize serializer (.get jedis (key-of prefix k))))
  
  (invoke [this k not_found]
    (if-let [v (.invoke this k)]
      v
      not_found))
  
  clojure.lang.IPersistentMap
  (assoc [this k v]
    (RedisPersistentMap. jedis prefix serializer (disj withouts k) (assoc assocMap k v)))
    
  (assocEx [this k v]
    (if (.containsKey this k)
      (throw (IllegalStateException. "Key already present"))
      (.assoc this k v)))
  
  (without [this k]
    (if (.containsKey this k)
      (RedisPersistentMap. jedis prefix serializer (conj withouts k) (dissoc assocMap k))
      this))
  
  java.lang.Iterable
  (iterator [this] (.iterator (entries-of jedis prefix serializer assocMap withouts)))
  
  clojure.lang.Associative
  (containsKey [this k]
    (cond 
      (contains? assocMap k)
      true
      (contains? withouts k)
      false
      :else
      (true? (.keys jedis (key-of prefix k)))))
  
  (entryAt [this k] (.get jedis (key-of prefix k)))
  
  clojure.lang.IPersistentCollection
  
  (count [_] 
    (let [ks (into #{} (map key-of (.keys jedis (key-of prefix "*"))))]
      (- 
        (+
          (count (filter #(not (contains? ks %)) (keys assocMap)))
          (count ks))
        (count withouts))))
               
  
  (cons [this [k v]] (dbg this))
  
  (empty [this] this)
  
  (equiv [this o]
    (if (= (.count this) (count o))
      (every? (fn [e] (= (val (dbg e)) (dbg (get o (dbg (key e)))))) this)
      false))
  
  clojure.lang.Seqable
  (seq [this] (.seq (entries-of jedis prefix serializer assocMap withouts)))
  
  
  clojure.lang.ILookup
  (valAt [this k]
    (cond 
      (contains? assocMap k)
      (get assocMap k)
      (contains? withouts k)
      nil
      :else
      (.deserialize serializer (.get jedis (key-of prefix k)))))
  
  (valAt [this k not_found] (if-let [v (.valAt this k)] v not_found)))



(defn redis-map [jedis prefix serializer]
  (RedisPersistentMap. jedis prefix serializer #{} {}))
