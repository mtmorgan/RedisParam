#' Creating the Redis backend
#'
#' @param job_name The job name used by the manager and workers to connect.
#' @param host The host of the Redis server.
#' @param port The port of the Redis server.
#' @param password The password of the redis server.
#' @param timeout The waiting time in `BLPOP`
#' @param type The type of the Backend(manager or worker?).
#' @param RedisParam RedisParam, if this argument is not NULL, all the other
#' arguments will be ignored except `type`.
RedisBackend <-
    function(job_name, host = "127.0.0.1", port = 6379L, password = NULL,
             timeout = 2592000L, type = c("manager", "worker"), RedisParam = NULL)
    {
        if(!is.null(RedisParam)){
            job_name <- bpjobname(RedisParam)
            host <- .redis_host(RedisParam)
            port <- .redis_port(RedisParam)
            password <- .redis_password(RedisParam)
            timeout <- bptimeout(RedisParam)
        }
        type <- match.arg(type)

        api_client <- hiredis(host = host,
                              port = as.integer(port),
                              password = password)
        api_client$CLIENT_SETNAME(paste0(job_name, "-redis_", type))
        job_queue <- paste0("biocparallel_redis_job:", job_name)
        result_queue <- paste0("biocparallel_redis_result:", job_name)

        .RedisBackend(api_client = api_client,
                     job_name = job_name,
                     job_queue = job_queue,
                     result_queue = result_queue,
                     timeout = as.integer(timeout),
                     type = type)
    }



.RedisBackend$methods(
    push = function(queue_name, value)
    {
        value <- serialize(value, NULL, xdr = FALSE)
        .self$api_client$RPUSH(
            key = queue_name,
            value = value
        )
    }
)

.RedisBackend$methods(
    pop = function(queue_name)
    {
        value <- NULL
        start_time <- Sys.time()
        repeat{
            value <- .self$api_client$BLPOP(
                key = queue_name,
                timeout = 1
            )
            if(!is.null(value)){
                break
            }
            wait_time <- difftime(Sys.time(), start_time, unit = 'secs')
            if(wait_time > self$timeout){
                stop("Redis pop operation timeout")
            }
        }
        unserialize(value[[2]])
    }
)

## push_* and pop_* depend on push and pop
.RedisBackend$methods(
    push_job = function(value)
    {
        .self$push(.self$job_queue, value)
    }
)

.RedisBackend$methods(
    pop_job = function(value)
    {
        .self$pop(.self$job_queue, value)
    }
)

.RedisBackend$methods(
    push_result = function(value)
    {
        .self$push(.self$result_queue, value)
    }
)

.RedisBackend$methods(
    pop_result = function()
    {
        .self$pop(.self$result_queue)
    }
)

.RedisBackend$methods(
    length = function()
    {
        if(identical(.self, .redis_NULL)){
            0
        } else {
            name <- paste0(x$jobname, "-redis_worker")
            worker_query <- sprintf(" name=%s ", name)
            clients <- .self$api_client$CLIENT_LIST()
            length(gregexpr(worker_query, clients)[[1]])
        }
    }
)


#' @export
setMethod(
    ".recv", "RedisBackend",
    function(worker)
    {
        worker$pop_job()
    })

#' @export
setMethod(
    ".send", "RedisBackend",
    function(worker, value)
    {
        worker$push_result(value)
    })

#' @export
setMethod(".close", "RedisBackend", function(worker) invisible(NULL) )

#' @export
setMethod(
    ".recv_any", "RedisBackend",
    function(backend)
    {
        value <- backend$pop_result()
        list(node = value$tag, value = value)
    })

#' @export
setMethod(
    ".send_to", "RedisBackend",
    function(backend, node, value)
    {
        ## ignore 'node'
        backend$push_job(value)
        TRUE
    })
