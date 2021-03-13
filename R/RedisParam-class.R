setOldClass("redis_worker")

setOldClass(c("redis_NULL", "redis_manager"))

.redis_NULL <-
    function()
{
    structure(list(), class = c("redis_NULL", "redis_manager"))
}

.RedisParam_prototype <- c(
    .BiocParallelParam_prototype,
    list(
        hostname = NA_character_, port = NA_integer_, backend = .redis_NULL(),
        is.worker = NA
    )
)

#' @import methods BiocParallel
.RedisParam <- setRefClass(
    "RedisParam",
    contains = "BiocParallelParam",
    fields = c(
        hostname = "character", port = "integer", backend = "redis_manager",
        is.worker = "logical"
    ),
    methods = list(
        show = function() {
            callSuper()
            cat(
                "  hostname: ", .redis_host(.self),
                "; port: ", .redis_port(.self), "\n",
                "  is.worker: ", .redis_isworker(.self), "\n",
                sep = "")
        }
    )
)

.redis_host <- function(x) x$hostname

.redis_port <- function(x) x$port

.redis_backend <- function(x) x$backend

.redis_set_backend <- function(x, value) {
    x$backend <- value
    invisible(x)
}

.redis_isworker <- function(x)
    x$is.worker

#' Enable redis-based parallel evaluation in BiocParallel
#'
#' @aliases .send,redis_worker-method .recv,redis_worker-method
#'     .close,redis_worker-method .send_to,redis_manager-method
#'     .recv_any,redis_manager-method
#'
#' @param workers integer(1) number of redis workers. For `is.worker
#'     = FALSE`, this parameter is the maximum number of workers
#'     expected to be available. For `is.worker = NA`, this is the
#'     number of workers opened by `bpstart()`.
#'
#' @param tasks See BiocParalleParam-class.
#' @param log See BiocParalleParam-class.
#' @param logdir See BiocParalleParam-class.
#' @param threshold See BiocParalleParam-class.
#' @param resultdir See BiocParalleParam-class.
#' @param stop.on.error See BiocParalleParam-class.
#' @param timeout See BiocParalleParam-class.
#' @param exportglobals See BiocParalleParam-class.
#' @param progressbar See BiocParalleParam-class.
#' @param RNGseed See BiocParalleParam-class.
#'
#' @param jobname character(1) name (unique) used to associate manager
#'     & workers on a queue.
#'
#' @param manager.hostname character(1) host name of redis server,
#'     from system environment variable `REDIS_HOST` or, by default,
#'     `"127.0.0.1"`.
#'
#' @param manager.port integer(1) port of redis server, from system
#'     environment variable `REDIS_PORT` or, by default, 6379.
#'
#' @param is.worker logical(1) \code{bpstart()} creates worker-only
#'     (\code{TRUE}), manager-only (\code{FALSE}), or manager and
#'     worker (\code{NA}, default) connections.
#'
#' @details Use an instance of `RedisParam()` for interactive parallel
#'     evaluation using `bplapply()` or `bpiterate()`. `RedisParam()`
#'     requires access to a redis server, running on
#'     `manager.hostname` (e.g., 127.0.0.1) on `manager.port` (e.g.,
#'     6379). `bplapply()` and `bpiterate()` will start and stop redis
#'     workers.
#'
#'     It may be convenient to use `bpstart()` and `bpstop()`
#'     independently, to amortize the cost of worker start-up across
#'     multiple calls to `bplapply()` / `bpiterate()`.
#'
#' @examples
#' if (requireNamespace("redux")) {
#'     res <- bplapply(1:20, function(i) Sys.getpid(), BPPARAM = RedisParam())
#'     table(unlist(res))
#' }
#'
#' @export
RedisParam <-
    function(workers = rpworkers(is.worker), tasks = 0L, jobname = ipcid(),
             log = FALSE, logdir = NA, threshold = "INFO",
             resultdir = NA_character_, stop.on.error= TRUE,
             timeout = 2592000L, exportglobals= TRUE,
             progressbar = FALSE, RNGseed = NULL,
             manager.hostname = rphost(), manager.port = rpport(),
             is.worker = NA)
{
    if (!is.null(RNGseed))
        RNGseed <- as.integer(RNGseed)

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
        progressbar = as.logical(progressbar),
        RNGseed = RNGseed,
        hostname = as.character(manager.hostname),
        port = as.integer(manager.port),
        is.worker = as.logical(is.worker)
    )
    do.call(.RedisParam, prototype)
}


#' @rdname RedisParam
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

#' @rdname RedisParam
#'
#' @export
rphost <-
    function()
{
    Sys.getenv("REDIS_HOST", "127.0.0.1")
}

#' @rdname RedisParam
#'
#' @export
rpport <-
    function()
{
    as.integer(Sys.getenv("REDIS_PORT", 6379))
}

#' @importFrom redux hiredis
.redis <-
    function(x, class)
{
    host <- .redis_host(x)
    port <- .redis_port(x)
    jobname <- bpjobname(x)
    job_queue <- paste0("biocparallel_redis_job:", jobname)
    result_queue <- paste0("biocparallel_redis_result:", jobname)
    timeout <- bptimeout(x)
    length <- bpnworkers(x)
    redis <- tryCatch({
        hiredis(host = host, port = port)
    }, error = function(e) {
        stop(
            "'redis' not available:\n",
            "  ", conditionMessage(e)
        )
    })
    redis$CLIENT_SETNAME(paste0(jobname, "-", class))
    structure(
        list(
            redis = redis,
            jobname = jobname,
            job_queue = job_queue,
            result_queue = result_queue,
            timeout = timeout, length = length
        ),
        class = class
    )
}

#' @export
setMethod(
    ".recv", "redis_worker",
    function(worker)
{
    result <- worker$redis$BLPOP(worker$job_queue, worker$timeout)
    unserialize(result[[2]])
})

#' @export
setMethod(
    ".send", "redis_worker",
    function(worker, value)
{
    value <- serialize(value, NULL, xdr = FALSE)
    worker$redis$RPUSH(worker$result_queue, value)
})

#' @export
setMethod(".close", "redis_worker", function(worker) invisible(NULL) )

#' @export
setMethod(
    ".recv_any", "redis_manager",
    function(backend)
{
    result <- backend$redis$BLPOP(backend$result_queue, backend$timeout)
    value <- unserialize(result[[2]])
    list(node = value$tag, value = value)
})

#' @export
setMethod(
    ".send_to", "redis_manager",
    function(backend, node, value)
{
    ## ignore 'node'
    value <- serialize(value, NULL, xdr = FALSE)
    backend$redis$RPUSH(backend$job_queue, value)
    TRUE
})

#' @export
length.redis_manager <-
    function(x)
{
    if (inherits(x, "redis_NULL")) {
        0L
    } else {
        name <- paste0(x$jobname, "-redis_worker")
        worker_query <- sprintf(" name=%s ", name)
        clients <- x$redis$CLIENT_LIST()
        length(gregexpr(worker_query, clients)[[1]])
    }
}

#' @importFrom redux redis_available hiredis
.bpstart_redis_manager <-
    function(x)
{
    redis <- .redis(x, "redis_manager")
    x <- .redis_set_backend(x, redis)
}

.bpstart_redis_worker_only <-
    function(x)
{
    worker <- .redis(x, "redis_worker")
    .bpworker_impl(worker)              # blocking
}

#' @importFrom parallel mcparallel
.bpstart_redis_worker_multicore <-
    function(x)
{
    old_redisparam_jobname <- Sys.getenv("REDISPARAM_JOBNAME")
    Sys.setenv(REDISPARAM_JOBNAME = bpjobname(x))
    on.exit({
        if (nzchar(old_redisparam_jobname)) {
            Sys.setenv(REDISPARAM_JOBNAME = old_redisparam_jobname)
        } else {
            Sys.unsetenv("REDISPARAM_JOBNAME")
        }
    })

    rscript <- R.home("bin/Rscript")
    script <- system.file(package="RedisParam", "script", "worker_start.R")
    for (i in seq_len(bpnworkers(x)))
        system2(rscript, script, wait = FALSE)
}

#' @rdname RedisParam
#'
#' @param x A `RedisParam` instance.
#' @export
setMethod(
    "bpisup", "RedisParam",
    function(x)
{
    !identical(bpbackend(x), .redis_NULL())
})

#' @rdname RedisParam
#'
#' @export
setMethod("bpbackend", "RedisParam", .redis_backend)

#' @rdname RedisParam
#'
#' @param \dots ignored.
#'
#' @export
setMethod(
    "bpstart", "RedisParam",
    function(x, ...)
{
    worker <- .redis_isworker(x)
    if (isTRUE(worker)) {
        ## worker only
        .bpstart_redis_worker_only(x)
    } else if (isFALSE(worker)) {
        ## manager only
        .bpstart_redis_manager(x)
        .bpstart_impl(x)
    } else {
        ## worker & manager
        .bpstart_redis_manager(x)
        .bpstart_redis_worker_multicore(x)
        .bpstart_impl(x)
    }
})

#' @rdname RedisParam
#'
#' @export
setMethod(
    "bpstop", "RedisParam",
    function(x)
{
    worker <- .redis_isworker(x)
    if (isTRUE(worker)) {
        ## no-op
    } else if (isFALSE(worker)) {
        ## don't stop workers by implicitly setting bpisup() to FALSE
        x <- .redis_set_backend(x, .redis_NULL())
        x <- .bpstop_impl(x)
    } else {
        ## stop workers
        x <- .bpstop_impl(x)
        x <- .redis_set_backend(x, .redis_NULL())
    }
    gc()                                # close connections
    x
})

#' @rdname RedisParam
#'
#' @export
setGeneric("bpstopall", function(x) standardGeneric("bpstopall"))

#' @rdname RedisParam
#'
#' @export
setMethod(
    "bpstopall", "RedisParam",
    function(x)
{
    if (!bpisup(x))
        return(x)

    worker <- .redis_isworker(x)
    if (isTRUE(worker)) {
        stop("use 'bpstopall()' from manager, not worker")
    } else {
        .bpstop_impl(x)                 # send 'DONE' to all workers
        .redis_set_backend(x, .redis_NULL())
    }
    gc()
    x
})

#' @rdname RedisParam
#'
#' @export
setMethod(
    "bpworkers", "RedisParam",
    function(x)
{
    if (is.na(.redis_isworker(x))) {
        x$workers
    } else {
        length(bpbackend(x))
    }
})
