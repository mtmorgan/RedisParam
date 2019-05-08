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
        hostname = "127.0.0.1", port = 6379L, backend = .redis_NULL(),
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
                "  hostname: ", .redis_host(.self), "\n",
                "  port: ", .redis_port(.self), "\n",
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
#' @param workers See BiocParalleParam-class.
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
#' @param manager.hostname character(1) host name of redis server.
#'
#' @param manager.port integer(1) port of redis server.
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
    function(workers = snowWorkers(), tasks = 0L, jobname = ipcid(),
             log = FALSE, logdir = NA, threshold = "INFO",
             resultdir = NA_character_, stop.on.error= TRUE,
             timeout = 2592000L, exportglobals= TRUE,
             progressbar = FALSE, RNGseed = NULL,
             manager.hostname = "127.0.0.1", manager.port = 6379L,
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
    structure(
        list(
            redis = redis,
            job_queue = job_queue, result_queue = result_queue,
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
    x$length
}

#' @importFrom redux redis_available hiredis
.bpstart_redis_manager <-
    function(x)
{
    redis <- .redis(x, "redis_manager")
    .redis_set_backend(x, redis)
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
    for (i in seq_len(bpnworkers(x))) {
        mcparallel({
            worker <- .redis(x, "redis_worker")
            .bpworker_impl(worker)
        }, detached = TRUE)
    }
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
        stop("'bpstopall()' requires 'bpisup()' to be TRUE")
    worker <- .redis_isworker(x)
    if (isTRUE(worker)) {
        stop("use 'bpstopall()' from manager, not worker")
    } else {
        stop("'bpstopall' not yet implemented")
        ## FIXME: how many signals to send?
        redis <- bpbackend(x)$redis
        .bpstop_impl(x)                 # send 'DONE' to all workers
        .redis_set_backend(x, .redis_NULL())
    }
    gc()
    x
})
