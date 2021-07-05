get.log.name <- function(x){
    paste0("RedisParam.", bpjobname(x))
}
get.log.file <- function(x){
    if(is.na(.is.worker(x))||!.is.worker(x)){
        paste0("RedisParam_manager_log_", Sys.getpid())
    }else{
        paste0("RedisParam_worker_log_", Sys.getpid())
    }
}

config.logger <- function(x){
    logger.name <- get.log.name(x)
    if(bplog(x)){
        set.log.threshold(x)
        if(!is.na(bplogdir(x))){
            filename <- get.log.file(x)
            flog.appender(appender.file(filename),
                          name = logger.name)
        }
    }
}

set.log.threshold <- function(x){
    threshold <- bpthreshold(x)
    logger.name <- get.log.name(x)
    if(threshold == "ERROR"){
        flog.threshold(ERROR, name= logger.name)
    }
    if(threshold == "WARN"){
        flog.threshold(WARN, name= logger.name)
    }
    if(threshold == "DEBUG"){
        flog.threshold(DEBUG, name= logger.name)
    }
    if(threshold == "INFO"){
        flog.threshold(INFO, name= logger.name)
    }
}

.debug <- function(x, ...)
{
    flog.debug(..., name = get.log.name(x))
}

.info <- function(x, ...)
{
    flog.info(..., name = get.log.name(x))
}

.warn <- function(x, ...)
{
    value <- flog.warn(..., name = get.log.name(x))
    warning(value)
}

.error <- function(x, ...)
{
    value <- flog.error(..., name = get.log.name(x))
    stop(value)
}




