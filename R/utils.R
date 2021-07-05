get.logger.name <- function(x){
    paste0("RedisParam.", bpjobname(x))
}


.debug <- function(x, ...)
{
    flog.debug(..., name = get.logger.name(x))
}

.info <- function(x, ...)
{
    flog.info(..., name = get.logger.name(x))
}

.warn <- function(x, ...)
{
    value <- flog.warn(..., name = get.logger.name(x))
    warning(value)
}

.error <- function(x, ...)
{
    value <- flog.error(..., name = get.logger.name(x))
    stop(value)
}

redis.alive <- function(redisParam){
    tryCatch({
        hiredis(host = .redis_host(redisParam),
                port = .redis_port(redisParam),
                password = .redis_password(redisParam)
        )
        TRUE
    }, error = function(e) FALSE)
}



