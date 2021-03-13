jobname <- Sys.getenv("REDISPARAM_JOBNAME")
param <- RedisParam::RedisParam(jobname = jobname, is.worker = TRUE)
RedisParam::bpstart(param)

