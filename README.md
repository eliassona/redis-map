# redismap

Represents redis as a clojure persistent map.


## Usage

```clojure
(use 'redismap.core)
(import 'redis.clients.jedis.Jedis)
(def m (redis-map (Jedis.))) ;serialize to json by default
;do map stuff
(def nm (assoc m "x" 10))
;store the change in redis
(store! nm)

```

FIXME

## License

Copyright © 2017 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
