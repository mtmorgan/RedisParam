redis.call("lpush", KEYS[1], KEYS[4], ARGV[1], "")
redis.call("lpush", KEYS[2], KEYS[1])
redis.call("lpush", KEYS[3], KEYS[1])
return nil
