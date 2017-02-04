(ns redismap.core
  (:require [clojure.data.json :as json]
            [storagemap.core :refer [IStorage ISerializer IPersistence storage-map]])
  (:import [[redis.clients.jedis Jedis]]
           [clojure.lang MapEntry]
           [java.util AbstractMap$SimpleEntry]
           [redis.clients.jedis Jedis]
           [storagemap.core StoragePersistentMap]
           )
  )


(extend-type Jedis
  IStorage
 (s-write! [this k v] (.set this k v)) 
 (s-read [this k] (.get this k))
 (s-delete! [this k] (.del this k))
 (s-keys [this query] (.keys this query)))

(deftype JsonSerializer []
  ISerializer
  (serialize [this data] (json/json-str data))
  (deserialize [this data] (json/read-str data)))
  
(defn redis-map 
  ([jedis prefix serializer]
    (storage-map jedis prefix serializer))
  ([jedis prefix]
    (redis-map jedis prefix (JsonSerializer.)))
  ([jedis]
    (redis-map jedis "p")))
