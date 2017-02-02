(ns redismap.core-test
  (:use [clojure.pprint])
  (:require [clojure.test :refer :all]
            [redismap.core :refer :all]
            [clojure.data.json :as json])
  (:import [redis.clients.jedis Jedis]
           [redismap.core RedisPersistentMap JsonSerializer]))


(defmacro def-redis-test [name prefix & code]
  `(deftest ~name
     (let [~'j (Jedis.)
           ~'rm (redis-map ~'j ~prefix (JsonSerializer.))]
       (try 
         (.flushAll ~'j)
         ~@code
         (finally 
           (.close ~'j)))))) 
  

(def-redis-test count-keys "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (is (= 1 (count rm))))


(def-redis-test get-value "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (is (= {"a" 1, "b" 2} (get rm "a"))))

(def-redis-test equality "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (.set j "s:b" (json/json-str {"x" 1, "y" 2}))
  (is (= {"a" {"a" 1, "b" 2}, "b" {"x" 1, "y" 2}} rm))
  (is (= rm {"a" {"a" 1, "b" 2}, "b" {"x" 1, "y" 2}})))
