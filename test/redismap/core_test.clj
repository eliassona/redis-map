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

(defn equivalence [expected actual]
  (is (= expected actual))
  (is (= actual expected))
  (is (= actual actual)))


(def-redis-test count-keys "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (is (= 1 (count rm))))


(def-redis-test get-value "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (is (= {"a" 1, "b" 2} (get rm "a"))))

(def-redis-test equality "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (.set j "s:b" (json/json-str {"x" 1, "y" 2}))
  (is (equivalence {"a" {"a" 1, "b" 2}, "b" {"x" 1, "y" 2}} rm)))


(def-redis-test assoc-test "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (.set j "s:b" (json/json-str {"x" 1, "y" 2}))
  (let [assoc-rm (assoc rm "c" 10)]
    (is (equivalence {"b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} rm))
    (is (equivalence {"c" 10, "b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} assoc-rm))))
    

(def-redis-test dissoc-test "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (.set j "s:b" (json/json-str {"x" 1, "y" 2}))
  (let [dissoc-rm (dissoc rm "a")]
    (is (equivalence {"b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} rm))    
    (is (equivalence {"b" {"x" 1, "y" 2}} dissoc-rm))))


(def-redis-test assoc-dissoc-test "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (.set j "s:b" (json/json-str {"x" 1, "y" 2}))
  (let [assoc-rm (assoc rm "c" 10)
        dissoc-rm (dissoc rm "c")]
    (is (equivalence {"b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} rm))
    (is (equivalence {"c" 10, "b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} assoc-rm))
    (is (equivalence {"b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} dissoc-rm))))


(def-redis-test persistence-test "s:"
  (.set j "s:a" (json/json-str {"a" 1, "b" 2}))
  (.set j "s:b" (json/json-str {"x" 1, "y" 2}))
  (let [assoc-rm (assoc rm "c" 10)
        dissoc-rm (dissoc rm "b")]
    (store! dissoc-rm)
    (is (equivalence {"a" {"a" 1, "b" 2}} rm))
    (is (equivalence {"a" {"a" 1, "b" 2}} (redis-map (Jedis.) "s:" (JsonSerializer.))))
    ))
    
    



