host <- Sys.getenv("REDISPARAM_HOST")
password <- Sys.getenv("REDISPARAM_PASSWORD")
port <- as.integer(Sys.getenv("REDISPARAM_PORT"))
jobname <- Sys.getenv("REDISPARAM_JOBNAME")
id <- Sys.getenv("REDISPARAM_ID")

param <- RedisParam::RedisParam(
    jobname = jobname,
    redis.hostname = host,
    redis.port = port,
    redis.password = password,
    is.worker = TRUE
)

RedisParam::bpstart(param)
