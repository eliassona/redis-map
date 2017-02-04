(defproject redismap "0.1.0-SNAPSHOT"
  :description "Redis persistent map"
  :url "https://github.com/eliassona/redis-map"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha10"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [redis.clients/jedis "2.9.0"]
                 [org.clojure/data.json "0.2.4"]
                 [storagemap "0.2.0-SNAPSHOT"]])
