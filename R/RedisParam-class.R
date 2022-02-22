.RedisParam <- setRefClass(
    "RedisParam",
    contains = "BiocParallelParam",
    fields = c(
        hostname = "character", port = "integer", password = "character",
        backend = "RedisBackend", is.worker = "logical",
        queue.multiplier = "numeric", attached.worker = "character"
    )
)

.RedisParam_prototype <- c(
    .BiocParallelParam_prototype,
    list(
        hostname = NA_character_, port = NA_integer_, password = NA_character_,
        backend = .redisNULL(), is.worker = NA, queue.multiplier = 2,
        attached.worker = NA_character_
    )
)

#' @rdname RedisParam-class
#'
#' @title Enable redis-based parallel evaluation in BiocParallel
#'
#' @param workers integer(1) number of redis workers. For `is.worker
#'     = FALSE`, this parameter is the maximum number of workers
#'     expected to be available. For `is.worker = NA`, this is the
#'     number of workers opened by `bpstart()`.
#'
#' @param tasks See `?"BiocParallelParam-class"`.
#'
#' @param log See `?"BiocParallelParam-class"`.
#'
#' @param logdir See `?"BiocParallelParam-class"`.
#'
#' @param threshold See `?"BiocParallelParam-class"`.
#'
#' @param resultdir See `?"BiocParallelParam-class"`.
#'
#' @param stop.on.error See `?"BiocParallelParam-class"`.
#'
#' @param timeout See `?"BiocParallelParam-class"`.
#'
#' @param exportglobals See `?"BiocParallelParam-class"`.
#'
#' @param progressbar See `?"BiocParallelParam-class"`.
#'
#' @param RNGseed See `?"BiocParallelParam-class"`.
#'
#' @param jobname character(1) name (unique) used to associate manager
#'     & workers on a queue.
#'
#' @param queue.multiplier numeric(1), The multiplier of the queue depth.
#'     The depth of the queue is calculated by `queue.multiplier * bpnworkers(p)`.
#'     A proper queue depth can provide more performance benefit in task
#'     dispatching, but the improvement is likely to be marginal for an excessively
#'     large `queue.multiplier`.
#'
#' @param redis.hostname character(1) host name of redis server,
#'     from system environment variable `REDISPARAM_HOST` or `REDIS_HOST`,
#'     if both are not defined, the default `"127.0.0.1"` is used.
#'
#' @param redis.port integer(1) port of redis server, from system
#'     environment variable `REDISPARAM_PORT` or `REDIS_PORT`,
#'     if both are not defined, the default `6379` is used.
#'
#' @param redis.password character(1) or NULL, host password of redis server
#'     from system environment variable `REDISPARAM_PASSWORD` or `REDIS_PASSWORD`,
#'     if both are not defined, the default `NA_character_` (no password) is used.
#'
#' @param is.worker logical(1) \code{bpstart()} creates worker-only
#'     (\code{TRUE}), manager-only (\code{FALSE}), or manager and
#'     worker (\code{NA}, default) connections.
#'
#' @details Use an instance of `RedisParam()` for interactive parallel
#'     evaluation using `bplapply()` or `bpiterate()`. `RedisParam()`
#'     requires access to a redis server, running on
#'     `manager.hostname` (e.g., 127.0.0.1) at `manager.port` (e.g.,
#'     6379). The manager and workers communicate via the redis
#'     server, rather than the socket connections used by other
#'     BiocParallel back-ends.
#'
#'     When invoked with `is.worker = NA` (the default) `bpstart()`,
#'     `bplapply()` and `bpiterate()` start and stop redis workers on
#'     the local computer. It may be convenient to use `bpstart()`
#'     and `bpstop()` independently, to amortize the cost of worker
#'     start-up across multiple calls to `bplapply()` / `bpiterate()`.
#'
#'     Alternatively, a manager and one or more workers can each be
#'     started in different processes across a network. The manager is
#'     started, e.g., in an interactive session, by specifying
#'     `is.worker = FALSE`. Workers are started, typically as
#'     background processes, with `is.worker = TRUE`. Both manager and
#'     workers must specify the same value for `jobname =`, the redis
#'     key used for communication. In this scenario, workers can be
#'     added at any time, including during e.g., `bplapply()`
#'     evaluation on the manager. See the vignette for possible
#'     scenarios.
#'
#' @examples
#' param <- RedisParam()
#' if (rpalive(param)) {
#'     res <- bplapply(1:20, function(i) Sys.getpid(), BPPARAM = param)
#'     table(unlist(res))
#' }
#'
#' @export
RedisParam <-
    function(
        workers = rpworkers(is.worker), tasks = 0L, jobname = ipcid(),
        log = FALSE, logdir = NA, threshold = "INFO",
        resultdir = NA_character_, stop.on.error = TRUE,
        timeout = NA_integer_, exportglobals = TRUE,
        progressbar = FALSE, RNGseed = NULL,
        queue.multiplier = 2L,
        redis.hostname = rphost(), redis.port = rpport(),
        redis.password = rppassword(),
        is.worker = NA
    )
{
    if (!is.null(RNGseed))
        RNGseed <- as.integer(RNGseed)
    if (!nzchar(redis.password) || is.null(redis.password))
        redis.password <- NA_character_
    if (isTRUE(is.worker) && missing(jobname))
        warning("Job name is not specified!")

    prototype <- .prototype_update(
        .RedisParam_prototype,
        workers = as.integer(workers),
        tasks = as.integer(tasks),
        jobname = as.character(jobname),
        log = as.logical(log),
        logdir = as.character(logdir),
        threshold = as.character(threshold),
        resultdir = as.character(resultdir),
        stop.on.error = as.logical(stop.on.error),
        timeout = as.integer(timeout),
        exportglobals = as.logical(exportglobals),
        queue.multiplier = as.numeric(queue.multiplier),
        progressbar = as.logical(progressbar),
        RNGseed = RNGseed,
        hostname = as.character(redis.hostname),
        port = as.integer(redis.port),
        password = as.character(redis.password),
        is.worker = as.logical(is.worker)
    )
    x <- do.call(.RedisParam, prototype)
    config.logger(x)
    x
}

#' @rdname RedisParam-class
#'
#' @param x A `RedisParam` object.
#'
#' @details `rpalive()` tests whether it is possible to connect to a
#'     redis server using the host, port, and password in the
#'     `RedisParam` object.
#'
#' @export
rpalive <-
    function(x)
{
    tryCatch({
        password <- rppassword(x)
        if (is.na(password))
            password <- NULL
        hiredis(host = rphost(x), port = rpport(x), password = password)
        TRUE
    }, error = function(e) FALSE)
}
