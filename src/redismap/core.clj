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

(defprotocol IPersistence
  (store! [this]))


(deftype JsonSerializer []
  ISerializer
  (serialize [this data] (json/json-str data))
  (deserialize [this data] (json/read-str data)))
  
(defn key-of 
  ([prefix k]
    (str prefix k))
  ([prefixed-key]
   (second (clojure.string/split prefixed-key #":")))) 
   
(defn entries-of 
  ([jedis prefix serializer assocMap]
  (loop [entries []
         assoc-map assocMap
         ks (.keys jedis (key-of prefix "*"))]
    (if (not (empty? ks))
      (let [pk (first ks)
            k (key-of pk)]
        (if (contains? assocMap k)
          (recur (conj entries (AbstractMap$SimpleEntry. k (get assocMap k))) (dissoc assoc-map k) (rest ks))
          (recur (conj entries (AbstractMap$SimpleEntry. k (.deserialize serializer (.get jedis pk)))) assoc-map (rest ks))))
      [entries assoc-map])))
  ([jedis prefix serializer assocMap withouts]
    (let [[entries assoc-map] (entries-of jedis prefix serializer assocMap)]
      (concat 
        (map #(AbstractMap$SimpleEntry. (key %) (val %)) assoc-map)
        (filter
          (fn [e]
            (not (contains? withouts (.getKey e))))
          entries)))))



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
      (= (count (.keys jedis (key-of prefix k))) 1)))
  
  (entryAt [this k] (MapEntry. k (.get jedis (key-of prefix k))))
  
  clojure.lang.IPersistentCollection
  
  (count [_] 
    (let [ks (into #{} (map key-of (.keys jedis (key-of prefix "*"))))]
      (- 
        (+
          (count (filter #(not (contains? ks %)) (keys assocMap)))
          (count ks))
        (count withouts))))
               
  
  (cons [this [k v]] (dbg this))
  
  (empty [this] 
    (RedisPersistentMap. jedis prefix serializer (into #{} (keys this)) {}))
  
  (equiv [this o]
    (if (= (.count this) (count o))
      (every? (fn [e] (= (val e) (get o (key e)))) this)
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
  
  (valAt [this k not_found] (if-let [v (.valAt this k)] v not_found))

  clojure.lang.MapEquivalence
  
  java.util.Map
  (size [this] (.count this))
  (isEmpty [this] (<= (.count this)))
  (get [this k] (.valAt this k))
  
  IPersistence
  (store! [this]
    (doseq [e assocMap]
      (.set jedis (key-of prefix (key e)) (.serialize serializer (val e))))
    (doseq [k withouts]
      (.del jedis (key-of prefix k)))
      
    )
  
  )



(defn redis-map [jedis prefix serializer]
  (RedisPersistentMap. jedis prefix serializer #{} {}))
