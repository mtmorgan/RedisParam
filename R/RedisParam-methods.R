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
           rphost(x), rpport(x), rppassword(x))
    worker <- RedisBackend(RedisParam = x, type = "worker")
    .bpworker_impl(worker)              # blocking
}

.bpstart_redis_worker_in_background <- function(x)
{
    .info(x, "starting %d worker(s) in the background", bpnworkers(x))
    worker_env <- list(
        REDISPARAM_HOST = rphost(x),
        REDISPARAM_PASSWORD = rppassword(x),
        REDISPARAM_PORT = rpport(x),
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

redis.alive <- function(x){
    tryCatch({
        hiredis(host = rphost(x),
                port = rpport(x),
                password = rppassword(x)
        )
        TRUE
    }, error = function(e) FALSE)
}

#########################
# Class methods
#########################
.RedisParam$methods(
    show = function() {
        callSuper()
        running.workers <- length(bpbackend(rp))
        if (is.null(rppassword(.self)))
            .password <- NA_character_
        else
            .password <- "*****"

        cat(
            "  hostname: ", rphost(.self), "\n",
            "  port: ", rpport(.self),
            "; password: ", .password,
            "; is.worker: ", rpisworker(.self), "\n",
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
        .trace(x, "bpisup")
        !identical(bpbackend(x), .redisNULL())
    }
)

#' @rdname RedisParam-class
#'
#' @export
setMethod("bpbackend", "RedisParam",
          function(x)
          {
              .trace(x, "bpbackend")
              x$backend
          }
)

#' @rdname RedisParam-class
#'
#' @export
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
setMethod(
    "bpstart", "RedisParam",
    function(x, ...)
    {
        .trace(x, "bpstart")
        if(!bpisup(x)){
            if(!redis.alive(x)){
                .error(x, "Fail to connect with the redis server")
            }
            worker <- rpisworker(x)
            .debug("isworker: %d", worker)
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
        .trace(x, "bpstop")
        worker <- rpisworker(x)
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
        .trace(x, "bpstopall")
        if (!bpisup(x))
            return(x)


        worker <- rpisworker(x)
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
        if (is.na(rpisworker(x))) {
            x$workers
        } else {
            length(bpbackend(x))
        }
    }
)






