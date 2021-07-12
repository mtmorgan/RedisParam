## The following line influences file collation order, and thus
## presentation of functions on the help page.

#' @include RedisParam-accessors.R
NULL

#########################
# internal functions
#########################
.bpstart_redis_manager <-
    function(x)
{
    .info(x, "Setting the Redis manager backend")
    redis <- RedisBackend(RedisParam = x, type = "manager")
    bpbackend(x) <- redis
    x
}

.bpstart_redis_worker_only <-
    function(x)
{
    .info(x, "Starting the worker in the foreground")
    .debug(
        x,
        "Listening the Redis server from the host %s, port %d with the password %s",
        rphost(x), rpport(x), rppassword(x)
    )
    worker <- RedisBackend(RedisParam = x, type = "worker")
    .bpworker_impl(worker)              # blocking
}

.bpstart_redis_worker_in_background <-
    function(x)
{
    nworkers <- bpnworkers(x)
    .info(x, "starting %d worker(s) in the background", nworkers)

    redisIds <- vapply(seq_len(nworkers), function(i) ipcid(), character(1))
    worker_env <- list(
        REDISPARAM_HOST = rphost(x),
        REDISPARAM_PASSWORD = rppassword(x),
        REDISPARAM_PORT = rpport(x),
        REDISPARAM_JOBNAME = bpjobname(x)
    )

    worker_env <- worker_env[!vapply(worker_env, is.null, logical(1))]

    rscript <- R.home("bin/Rscript")
    script <- system.file(package="RedisParam", "script", "worker_start.R")
    for (i in seq_len(nworkers)) {
        worker_env$REDISPARAM_ID <- redisIds[i]
        withr::with_envvar(
            worker_env,
            system2(rscript, shQuote(script), stdout = FALSE, wait = FALSE)
        )
    }

    .trace(x, "Waiting for the workers")
    ## Wait until all workers are running
    startTime <- Sys.time()
    repeat {
        success <- redisIds %in% bpworkers(bpbackend(x))
        if (all(success)) {
            break
        }
        if (difftime(Sys.time(),startTime, units = "secs") > 10) {
            if (sum(success) == 0) {
                .error(x, "Fail to start the worker in the background")
            } else {
                .warn(
                    x, "Continue with %d workers (Expected: %d).",
                    sum(success), length(success)
                )
            }
        }
        Sys.sleep(0.5)
    }
}

#########################
# Class methods
#########################
.RedisParam$methods(
    show = function() {
        callSuper()
        ## Temporary disable the log
        if(bplog(.self)){
            bplog(.self) <- FALSE
            on.exit(
                bplog(.self) <- TRUE
            )
        }

        running.workers <- length(bpbackend(.self))
        if (is.null(rppassword(.self)))
            .password <- NA_character_
        else
            .password <- "*****"
        cat(
            "  rphost: ", rphost(.self),
            "; rpport: ", rpport(.self),
            "; rppassword: ", .password, "\n",
            "  rpisworker: ", rpisworker(.self),
            if (!isTRUE(rpisworker(.self)))
                paste0("; running workers: ", bpnworkers(bpbackend(.self))),
            "\n",
            sep = "")
    }
)

#' @rdname RedisParam-class
#'
#' @export
setMethod("bpisup", "RedisParam",
    function(x)
{
    .trace(x, "bpisup")
    !identical(bpbackend(x), .redisNULL())
})

#' @rdname RedisParam-class
#'
#' @export
setMethod("bpbackend", "RedisParam",
    function(x)
{
    .trace(x, "bpbackend")
    x$backend
})

setReplaceMethod("bpbackend", c("RedisParam", "RedisBackend"),
    function(x, value)
{
    .trace(x, "bpbackend replacement")
    x$backend <- value
    x
})

#' @rdname RedisParam-class
#'
#' @param \dots ignored.
#'
#' @export
setMethod("bpstart", "RedisParam",
    function(x, ...)
{
    .trace(x, "bpstart")
    if (bpisup(x)) {
        return(invisible(x))
    }
    if (!rpalive(x)) {
        .error(x, "Fail to connect with the redis server")
    }
    worker <- rpisworker(x)
    .debug(x, "isworker: %d", worker)
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

    invisible(x)
})

#' @rdname RedisParam-class
#'
#' @export
setMethod("bpstop", "RedisParam",
    function(x)
{
    .trace(x, "bpstop")
    worker <- rpisworker(x)
    if (isTRUE(worker)) {
        ## no-op
    } else if (isFALSE(worker)) {
        ## don't stop workers by implicitly setting bpisup() to FALSE
        bpbackend(x) <- .redisNULL()
        x <- .bpstop_impl(x)
    } else {
        ## stop workers
        x <- .bpstop_impl(x)
        bpbackend(x) <- .redisNULL()
    }
    gc()                                # close connections

    invisible(x)
})

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
bpstopall <-
    function(x)
{
    stopifnot(is(x, "RedisParam"))
    .trace(x, "bpstopall")

    if (isTRUE(rpisworker(x))) {
        .error("use 'bpstopall()' from manager, not worker")
    }

    if (!bpisup(x)) {
        if (!rpalive(x))
            return(invisible(x))
        .bpstart_redis_manager(x)
    }

    .bpstop_impl(x)                 # send 'DONE' to all workers
    bpbackend(x) <- .redisNULL()
    gc()
    invisible(x)
}


setReplaceMethod("bplog", c("RedisParam", "logical"),
                 function(x, value)
                 {
                     x$log <- value
                     x
                 })

