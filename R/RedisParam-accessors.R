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

.is.worker <- function(x) x$is.worker

