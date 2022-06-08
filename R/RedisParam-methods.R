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
    fmt <- paste0(
        "Listening to the Redis server from host '%s', port '%d' and ",
        "password '%s'"
    )
    .debug(x, fmt, rphost(x), rpport(x), rppassword(x))

    ## If more than one worker is required
    ## start workers in the backgroud
    nworkers <- x$workers - 1L
    if (nworkers)
        .bpstart_redis_worker_in_background(x, nworkers, wait = FALSE)
    ## Block the current R session
    worker <- RedisBackend(RedisParam = x, type = "worker")
    on.exit(.QUIT(worker))
    .bpworker_impl(worker)              # blocking
}

.bpstart_redis_worker_in_background <-
    function(x, nworkers = x$workers, wait = TRUE)
{
    .info(x, "starting %d worker(s) in the background", nworkers)

    redisIds <- vapply(seq_len(nworkers), function(i) .randomId(), character(1))
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

    if(!wait)
        return()

    .trace(x, "Waiting for the workers")
    ## Wait until all workers are running
    startTime <- Sys.time()
    repeat {
        success <- redisIds %in% bpworkers(bpbackend(x))
        if (all(success)) {
            break
        }
        if (difftime(Sys.time(), startTime, units = "secs") > 10) {
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
    rpattachedworker(x) <- redisIds[success]
}

#########################
# Class methods
#########################
.RedisParam$methods(
    show = function() {
        callSuper()
        ## Temporarily disable the log
        if (bplog(.self)) {
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
    } else {
        if (bpisup(x)) {
            ## stop the workers which are started by the manager
            workers <- rpattachedworker(x)
            if (length(workers) >1 || !is.na(workers)) {
                if (!bpisup(x))
                    .bpstart_redis_manager(x)
                backend <- bpbackend(x)
                workers <- intersect(workers, bpworkers(x))
                for (i in workers)
                    .send_to(backend, i, BiocParallel:::.DONE())
                rpattachedworker(x) <- NA_character_
            }

            ## Remove the leaking memory
            .cleanMissingWorkers(bpbackend(x))
        }
        ## Do not stop the other workers
        bpbackend(x) <- .redisNULL()
        x <- .bpstop_impl(x)
    }

    gc()                                # close connections
    invisible(x)
})

## bpworkers must return the correct worker number
## for making the task division work correctly
#' @rdname RedisParam-class
#'
#' @export
setMethod("bpworkers", "RedisParam",
    function(x)
{
    if (bpisup(x)) {
        bpworkers(bpbackend(x))
    }else{
        callNextMethod()
    }
})

#' @rdname RedisParam-class
#'
#' @param value The value you want to replace with
#' @export
setReplaceMethod("bplog", c("RedisParam", "logical"),
    function(x, value)
{
    if (bpisup(x)) {
        bpbackend(x)$log <- value
    }
    x$log <- value
    x
})

rpattachedworker <-
    function(x)
{
    x$attached.worker
}

`rpattachedworker<-` <-
    function(x, value)
{
    x$attached.worker <- value
    x
}
