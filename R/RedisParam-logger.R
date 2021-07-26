## Get the logger name
get.logger.name <-
    function(x)
{
    paste0("RedisParam.", bpjobname(x))
}

## Get the file used by the logger
get.log.file <-
    function(x)
{
    if (is.na(rpisworker(x))||!rpisworker(x)) {
        paste0("RedisParam_manager_log_", Sys.getpid())
    } else {
        paste0("RedisParam_worker_log_", Sys.getpid())
    }
}

config.logger <-
    function(x)
{
    logger.name <- get.logger.name(x)
    if (bplog(x)) {
        set.log.threshold(x)
        if (!is.na(bplogdir(x))) {
            filename <- get.log.file(x)
            flog.appender(
                appender.file(filename),
                name = logger.name
            )
        }
    }
}

set.log.threshold <-
    function(x)
{
    threshold <- bpthreshold(x)
    logger.name <- get.logger.name(x)
    flog.threshold(get(threshold), name = logger.name)
}

.trace <-
    function(x, ...)
{
    if (!missing(x) && bplog(x))
        flog.trace(..., name = get.logger.name(x))
}

.debug <-
    function(x, ...)
{
    if (!missing(x) && bplog(x))
        flog.debug(..., name = get.logger.name(x))
}

.info <-
    function(x, ...)
{
    if (!missing(x) && bplog(x))
        flog.info(..., name = get.logger.name(x))
}

.warn <-
    function(x, fmt, ...)
{
    if (!missing(x) && bplog(x)) {
        value <- flog.warn(fmt, ..., name = get.logger.name(x))
    } else {
        value <- sprintf(fmt, ...)
    }
    warning(value, call. = FALSE)
}

.error <-
    function(x, fmt, ...)
{
    if (!missing(x) && bplog(x)) {
        value <- flog.error(fmt, ..., name = get.logger.name(x))
    } else {
        value <- sprintf(fmt, ...)
    }
    stop(value, call. = FALSE)
}
