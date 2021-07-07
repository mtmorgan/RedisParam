.redisNULL <- function()
{
    structure(list(), class = c("redisNULL", "RedisBackend"))
}

#' Creating the Redis backend
#'
#' @param jobname The job name used by the manager and workers to connect.
#' @param host The host of the Redis server.
#' @param port The port of the Redis server.
#' @param password The password of the redis server.
#' @param timeout The waiting time in `BLPOP`
#' @param type The type of the Backend(manager or worker?).
#' @param RedisParam RedisParam, if this argument is not NULL, all the other
#' arguments will be ignored except `type`.
RedisBackend <- function(
    RedisParam = NULL, jobname = "",
    host = "127.0.0.1", port = 6379L, password = NULL,
    timeout = 2592000L, type = c("manager", "worker")
)
{
    if (!is.null(RedisParam)) {
        jobname <- bpjobname(RedisParam)
        host <- rphost(RedisParam)
        port <- rpport(RedisParam)
        password <- rppassword(RedisParam)
        timeout <- bptimeout(RedisParam)
    }
    type <- match.arg(type)
    id <- Sys.getenv("REDISPARAM_ID", ipcid())
    api_client <- hiredis(
        host = host,
        port = as.integer(port),
        password = password)
    clientName <- .client_name(jobname, type, id)
    api_client$CLIENT_SETNAME(clientName)
    job_queue <- paste0("biocparallel_redis_job:", jobname)
    result_queue <- paste0("biocparallel_redis_result:", jobname)

    structure(
        list(
            api_client = api_client,
            jobname = jobname,
            job_queue = job_queue,
            result_queue = result_queue,
            timeout = as.integer(timeout),
            type = type,
            id = id),
        class = "RedisBackend")
}

.client_name_prefix <- function(jobname, type){
    paste0(jobname, "_redis_", type, "_")
}

.client_name <- function(jobname, type, id)
{
    paste0(.client_name_prefix(jobname, type), id)
}


## regmatches
.all_workers <- function(x){
    prefix <- .client_name_prefix(x$jobname, "worker")
    clients <- x$api_client$CLIENT_LIST()
    idx <- gregexpr(paste0(" name=", prefix, ".+? "), clients)
    clientsNames <- regmatches(clients, idx)[[1]]
    ## Remove the prefix and the tailing space
    substring(clientsNames, nchar(prefix) + 7, nchar(clientsNames) - 1)
}


.push <- function(x, queue_name, value)
{
    value <- serialize(value, NULL, xdr = FALSE)
    x$api_client$RPUSH(
        key = queue_name,
        value = value
    )
}

.pop <- function(x, queue_name)
{
    value <- NULL
    start_time <- Sys.time()
    repeat{
        value <- x$api_client$BLPOP(
            key = queue_name,
            timeout = 1
        )
        if (!is.null(value)) {
            break
        }
        wait_time <- difftime(Sys.time(), start_time, unit = 'secs')
        if (wait_time > x$timeout) {
            stop("Redis pop operation timeout")
        }
    }
    unserialize(value[[2]])
}

## push_* and pop_* depend on push and pop
.push_job <- function(x, value)
{
    .push(x, x$job_queue, value)
}

.pop_job <- function(x)
{
    .pop(x, c(x$job_queue, x$id))
}

.push_result <- function(x, value)
{
    .push(x, x$result_queue, value)
}

.pop_result <- function(x)
{
    .pop(x, x$result_queue)
}

#' @export
length.RedisBackend <- function(x)
{
    length(bpworkers(x))
}


## Worker
#' @export
setMethod(
    ".recv", "RedisBackend",
    function(worker)
    {
        .pop_job(worker)
    }
)

#' @export
setMethod(
    ".send", "RedisBackend",
    function(worker, value)
    {
        .push_result(worker, value)
    }
)

#' @export
setMethod(".close", "RedisBackend", function(worker) {
    invisible(NULL)
})

## Manager
#' @export
setMethod(
    ".recv_any", "RedisBackend",
    function(backend)
    {
        value <- .pop_result(backend)
        list(node = value$tag, value = value)
    }
)


#' @export
setMethod(
    ".send_to", "RedisBackend",
    function(backend, node, value)
    {
        node <- bpworkers(backend)[node]
        .push(backend, node, value)
        TRUE
    }
)


setMethod(bpjobname, "RedisBackend",
          function(x)
          {
              x$jobname
          }
)

setMethod(bpworkers, "RedisBackend",
          function(x)
          {
              if (identical(x, .redisNULL())) {
                  character()
              } else {
                  .all_workers(x)
              }
          }
)
