.RedisParam$methods(
    show = function() {
        callSuper()
        running.workers <- length(bpbackend(rp))
        .password <- "*****"
        if (is.null(.password(.self)))
            .password <- NA_character_
        cat(
            "  hostname: ", .host(.self), "\n",
            "  port: ", .port(.self),
            "; password: ", .password,
            "; is.worker: ", .is.worker(.self), "\n",
            sep = "")
        if(bpisup(.self)){
            cat("Running workers: ", running.workers, "\n")
        }
    }
)



#' @rdname RedisParam-class
#'
#' @param x A `RedisParam` instance.
#' @export
setMethod(
    "bpisup", "RedisParam",
    function(x)
    {
        !identical(bpbackend(x), .redisNULL())
    }
)

#' @rdname RedisParam-class
#'
#' @export
setMethod("bpbackend", "RedisParam", .backend)

#' @rdname RedisParam-class
#'
#' @param \dots ignored.
#'
#' @export
setMethod(
    "bpstart", "RedisParam",
    function(x, ...)
    {
        .debug(x, "bpstart")
        if(!bpisup(x)){
            if(!redis.alive(x)){
                .error(x, "Fail to connect with the redis server")
            }
            worker <- .is.worker(x)
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
    }
)

#' @rdname RedisParam-class
#'
#' @export
setMethod(
    "bpstop", "RedisParam",
    function(x)
    {
        .debug(x, "bpstop")
        worker <- .is.worker(x)
        if (isTRUE(worker)) {
            ## no-op
        } else if (isFALSE(worker)) {
            ## don't stop workers by implicitly setting bpisup() to FALSE
            x <- .set.backend(x, .redisNULL())
            x <- .bpstop_impl(x)
        } else {
            ## stop workers
            x <- .bpstop_impl(x)
            x <- .set.backend(x, .redisNULL())
        }
        gc()                                # close connections
        TRUE
    }
)

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

        worker <- .is.worker(x)
        if (isTRUE(worker)) {
            .error("use 'bpstopall()' from manager, not worker")
        } else {
            .bpstop_impl(x)                 # send 'DONE' to all workers
            .set.backend(x, .redisNULL())
        }
        gc()
        x
    }
)

#' @rdname RedisParam-class
#'
#' @export
setMethod(
    "bpworkers", "RedisParam",
    function(x)
    {
        if (is.na(.is.worker(x))) {
            x$workers
        } else {
            length(bpbackend(x))
        }
    }
)






