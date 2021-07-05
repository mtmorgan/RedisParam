host <- Sys.getenv("REDISPARAM_HOST")
password <- Sys.getenv("REDISPARAM_PASSWORD")
port <- as.integer(Sys.getenv("REDISPARAM_PORT"))
jobname <- Sys.getenv("REDISPARAM_JOBNAME")

param <- RedisParam::RedisParam(
    jobname = jobname,
    manager.hostname = host,
    manager.port = port,
    manager.password = password,
    is.worker = TRUE
)

RedisParam::bpstart(param)

