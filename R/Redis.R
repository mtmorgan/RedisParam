## Lua method
.pipeline <-
    function(x, ..., .commands = list(...))
{
    x$redisClient$pipeline(..., .commands = .commands)
}

.LPUSH <-
    function(x, key, value)
{
    x$redisClient$LPUSH(key, value)
}

.BRPOP <- function(x, keys, timeout = 1L)
{
    x$redisClient$command(c("BRPOP", keys, timeout))
}

.HKEYS <- function(x, key)
{
    x$redisClient$HKEYS(key)
}

.HDEL <- function(x, key, fields)
{
    x$redisClient$command(c("HDEL", key, fields))
}

.SMEMBERS <- function(x, key)
{
    x$redisClient$SMEMBERS(key)
}

.SISMEMBER <- function(x, key, member)
{
    x$redisClient$SISMEMBER(key, member)
}

.DEL <- function(x, keys)
{
    x$redisClient$DEL(keys)
}

.FLUSHALL <-
    function(x)
{
    if (is(x, "RedisParam"))
        x <- bpbackend(x)
    if (identical(x, .redisNULL()))
        return(NULL)
    x$redisClient$FLUSHALL()
}

.QUIT <-
    function(x)
{
    x$redisClient$QUIT()
}
