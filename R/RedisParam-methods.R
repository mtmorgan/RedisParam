.redis_host <- function(x) x$hostname

.redis_port <- function(x) x$port

.redis_password <- function(x) {
    password <- x$password
    if (is.na(password)) {
        NULL
    } else {
        password
    }
}

.redis_backend <- function(x) x$backend

.redis_set_backend <- function(x, value) {
    x$backend <- value
    invisible(x)
}

.redis_isworker <- function(x)
    x$is.worker

.bpstart_redis_manager <-
    function(x)
    {
        redis <- RedisBackend(RedisParam = x, type = "manager")
        x <- .redis_set_backend(x, redis)
    }

.bpstart_redis_worker_only <-
    function(x)
    {
        worker <- RedisBackend(RedisParam = x, type = "worker")
        .bpworker_impl(worker)              # blocking
    }

.bpstart_redis_worker_in_background <-
    function(x)
    {
        worker_env <- list(
            REDISPARAM_PASSWORD = .redis_password(x),
            REDISPARAM_PORT = .redis_port(x),
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

#' @rdname RedisParam-class
#'
#' @param x A `RedisParam` instance.
#' @export
setMethod(
    "bpisup", "RedisParam",
    function(x)
    {
        !identical(bpbackend(x), .redis_NULL)
    })

#' @rdname RedisParam-class
#'
#' @export
setMethod("bpbackend", "RedisParam", .redis_backend)

#' @rdname RedisParam-class
#'
#' @param \dots ignored.
#'
#' @export
setMethod(
    "bpstart", "RedisParam",
    function(x, ...)
    {
        if(!bpisup(x)){
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
                .bpstart_redis_worker_in_background(x)
                .bpstart_impl(x)
            }
        }
        TRUE
    })

#' @rdname RedisParam-class
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
            x <- .redis_set_backend(x, .redis_NULL)
            x <- .bpstop_impl(x)
        } else {
            ## stop workers
            x <- .bpstop_impl(x)
            x <- .redis_set_backend(x, .redis_NULL)
        }
        gc()                                # close connections
        TRUE
    })

#' @rdname RedisParam-class
#'
#' @export
setGeneric("bpstopall", function(x) standardGeneric("bpstopall"))

#' @rdname RedisParam-class
#'
#' @details `bpstopall()` is used from the manager to stop redis
#'     workers launched independently, with `is.worker = TRUE`.
#'
#' @examples
#' \dontrun{
#' ## start workers in background proocess(es)
#' rscript <- R.home("bin/Rscript")
#' worker_script <- tempfile()
#' writeLines(c(
#'     'worker <- RedisParam::RedisParam(jobname = "demo", is.worker = TRUE)',
#'     'RedisParam::bpstart(worker)'
#' ), worker_script)
#'
#' for (i in seq_len(2))
#'     system2(rscript, worker_script, wait = FALSE)
#'
#' ## start manager
#' p <- RedisParam(jobname = "demo", is.worker = FALSE)
#' result <- bplapply(1:5, function(i) Sys.getpid(), BPPARAM = p)
#' table(unlist(result))
#'
#' ## stop all workers
#' bpstopall(p)
#' }
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
            .redis_set_backend(x, .redis_NULL)
        }
        gc()
        x
    })

#' @rdname RedisParam-class
#'
#' @export
setMethod(
    "bpworkers", "RedisParam",
    function(x)
    {
        if (is.na(.redis_isworker(x))) {
            x$workers
        } else {
            bpbackend(x)$length()
        }
    })
