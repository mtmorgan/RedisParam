#' @rdname RedisParam-class
#'
#' @details `rpworkers()` determines the number of workers using
#'     `snowWorkers()` if workers are created dynamically, or a fixed
#'     maximum (currently 1000) if workers are listening on a queue.
#'
#'     `rphost()` reads the host name of the redis server from a
#'     system environment variable `"REDIS_HOST"`, defaulting to
#'     `"127.0.0.1"`
#'
#'     `rpport()` reads the port of the redis server from a system
#'     environment variable `"REDIS_PORT"`, defaulting to 6379.
#'
#'     `rppassword()` reads an (optional) password from the system
#'     environment variable "REDIS_PASSWORD", defaulting to
#'     `NA_character_` (no password). The password is used by the
#'     redis AUTH command.
#'
#' @export
rpworkers <-
    function(is.worker)
    {
        stopifnot(is.logical(is.worker), length(is.worker) == 1L)
        if (is.na(is.worker)) {
            snowWorkers()
        } else if (is.worker) {
            1L
        } else {
            ## a large number -- up to 1000 listening on the queue
            1000L
        }
    }

#' @rdname RedisParam-class
#'
#' @export
rphost <-
    function()
    {
        Sys.getenv("REDIS_HOST", "127.0.0.1")
    }

#' @rdname RedisParam-class
#'
#' @export
rpport <-
    function()
    {
        as.integer(Sys.getenv("REDIS_PORT", 6379))
    }

#' @rdname RedisParam-class
#'
#' @export
rppassword <-
    function()
    {
        Sys.getenv("REDIS_PASSWORD",  NA_character_)
    }



#########################
# Accessors
#########################
.host <- function(x) x$hostname

.port <- function(x) x$port

.password <- function(x)
{
    password <- x$password
    if (is.na(password)) {
        NULL
    } else {
        password
    }
}

.backend <- function(x) x$backend

.set.backend <- function(x, value)
{
    x$backend <- value
    invisible(x)
}

.is.worker <- function(x) x$is.worker

.threshold <- function(x) x$threshold



#########################
# internal functions
#########################
.bpstart_redis_manager <- function(x)
{
    .info(x, "Setting the Redis manager backend")
    redis <- RedisBackend(RedisParam = x, type = "manager")
    x <- .set.backend(x, redis)
}

.bpstart_redis_worker_only <- function(x)
{
    .info(x, "Starting the worker in the foreground")
    .debug(x,
           "Listening the Redis server from the host %s, port %d with the password %s",
           .host(x), .port(x), .password(x))
    worker <- RedisBackend(RedisParam = x, type = "worker")
    .bpworker_impl(worker)              # blocking
}

.bpstart_redis_worker_in_background <- function(x)
{
    .info(x, "starting %d worker(s) in the background", bpnworkers(x))
    worker_env <- list(
        REDISPARAM_HOST = .host(x),
        REDISPARAM_PASSWORD = .password(x),
        REDISPARAM_PORT = .port(x),
        REDISPARAM_JOBNAME = bpjobname(x)
    )

    worker_env <- worker_env[!vapply(worker_env, is.null, logical(1))]
    withr::with_envvar(
        worker_env
        ,{
            rscript <- R.home("bin/Rscript")
            script <- system.file(package="RedisParam", "script", "worker_start.R")
            for (i in seq_len(bpnworkers(x)))
                system2(rscript, shQuote(script), wait = FALSE)
        })

}


enable.log <- function(x){
    threshold <- .threshold(x)
    logger.name <- get.logger.name(x)
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
