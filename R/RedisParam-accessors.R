## The following line influences file collation order, and thus
## presentation of functions on the help page.

#' @include RedisParam-class.R
NULL

#' @rdname RedisParam-class
#'
#' @details
#'     `rpworkers()` determines the number of workers using
#'     `snowWorkers()` if workers are created dynamically, or a fixed
#'     maximum (currently 1000) if workers are listening on a queue.
#'
#'     `rphost()` reads the host name of the redis server from a
#'     system environment variable `"REDIS_HOST"`, defaulting to
#'     `"127.0.0.1"`. `rphost(x)` gives the host name used by `x`.
#'
#'     `rpport()` reads the port of the redis server from a system
#'     environment variable `"REDIS_PORT"`, defaulting to 6379.
#'     `rpport(x)` gives the port used by `x`.
#'
#'     `rppassword()` reads an (optional) password from the system
#'     environment variable "REDIS_PASSWORD", defaulting to
#'     `NULL` (no password). `rppassword(x)` gives the password
#'      used by `x`.
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
    function(x)
{
    if (missing(x)) {
        Sys.getenv("REDIS_HOST", "127.0.0.1")
    } else {
        x$hostname
    }
}

#' @rdname RedisParam-class
#'
#' @export
rpport <-
    function(x)
{
    if (missing(x)) {
        value <- Sys.getenv("REDIS_PORT", "6379")
        port <- as.integer(value)
        if (is.na(port)) {
            .error(
                "The 'REDIS_PORT' environment variable cannot be coerced to an integer. The original value was '%s'.",
                value
            )
        }
    } else {
        port <- x$port
    }
    port
}

#' @rdname RedisParam-class
#'
#' @export
rppassword <-
    function(x)
{
    if (missing(x)) {
        Sys.getenv("REDIS_PASSWORD", NULL)
    } else {
        value <- x$password
        if (is.na(value)) {
            NULL
        } else {
            value
        }
    }
}

#' @rdname RedisParam-class
#'
#' @export
rpisworker <-
    function(x)
{
    x$is.worker
}
