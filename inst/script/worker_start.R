redis_password <- Sys.getenv("REDISPARAM_PASSWORD")
redis_port <- as.integer(Sys.getenv("REDISPARAM_PORT"))
jobname <- Sys.getenv("REDISPARAM_JOBNAME")


param <- RedisParam::RedisParam(
    jobname = jobname,
    manager.port = redis_port,
    manager.password = redis_password,
    is.worker = TRUE
)

RedisParam::bpstart(param)

