#' @importFrom logger log_appender appender_file appender_stdout
#'     log_formatter formatter_sprintf log_threshold log_trace
#'     log_debug log_info log_warn log_error

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
    log_formatter(formatter_sprintf, namespace = logger.name)
    log_appender(appender_stdout)
    if (bplog(x)) {
        set.log.threshold(x)
        if (!is.na(bplogdir(x))) {
            filename <- get.log.file(x)
            log_appender(
                appender_file(filename),
                namespace = logger.name
            )
        }
    }
}

set.log.threshold <-
    function(x)
{
    threshold <- bpthreshold(x)
    logger.name <- get.logger.name(x)
    log_threshold(get(threshold), namespace = logger.name)
}

.trace <-
    function(x, ...)
{
    if (!missing(x) && bplog(x))
        log_trace(..., namespace = get.logger.name(x))
}

.debug <-
    function(x, ...)
{
    if (!missing(x) && bplog(x))
        log_debug(..., namespace = get.logger.name(x))
}

.info <-
    function(x, ...)
{
    if (!missing(x) && bplog(x))
        log_info(..., namespace = get.logger.name(x))
}

.warn <-
    function(x, fmt, ...)
{
    if (!missing(x) && bplog(x)) {
        value <- log_warn(fmt, ..., namespace = get.logger.name(x))
    } else {
        value <- sprintf(fmt, ...)
    }
    warning(value, call. = FALSE)
}

.error <-
    function(x, fmt, ...)
{
    if (!missing(x) && bplog(x)) {
        value <- log_error(fmt, ..., namespace = get.logger.name(x))
    } else {
        value <- sprintf(fmt, ...)
    }
    stop(value, call. = FALSE)
}

.redisLog <-
    function(x, fmt, ...)
{
    if (!missing(x) && x$redis.log) {
        value <- sprintf(fmt, ...)
        x$redisClient$RPUSH(.redisLogQueue(x$jobname), value)
    }
}
