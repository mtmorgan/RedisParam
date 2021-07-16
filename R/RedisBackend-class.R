setOldClass(c("redisNULL", "RedisBackend"))

.RedisParam <- setRefClass(
    "RedisParam",
    contains = "BiocParallelParam",
    fields = c(
        hostname = "character", port = "integer", password = "character",
        backend = "RedisBackend", is.worker = "logical"
    )
)

.redisNULL <-
    function()
{
    structure(list(), class = c("redisNULL", "RedisBackend"))
}

#' @rdname RedisBackend-class
#'
#' @title Creating the Redis backend
#'
#' @keywords internal
#'
#' @param RedisParam RedisParam, if this argument is not NULL, all the
#'     other arguments will be ignored except `type`.
#'
#' @param jobname character(1) The job name used by the manager and
#'     workers to connect.
#'
#' @param host character(1) The host of the Redis server.
#'
#' @param port integer(1) The port of the Redis server.
#'
#' @param password character(1) The password of the redis server.
#'
#' @param timeout integer(1) The waiting time in `BLPOP`.
#'
#' @param type character(1) The type of the backend (manager or worker?).
RedisBackend <-
    function(
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
    clientName <- .clientName(jobname, type, id)
    jobQueue <- paste0("biocparallel_redis_job:", jobname)
    resultQueue <- paste0("biocparallel_redis_result:", jobname)
    workerList <-paste0("biocparallel_redis_workers:", jobname)


    x <- structure(
        list(
            api_client = api_client,
            jobname = jobname,
            jobQueue = jobQueue,
            resultQueue = resultQueue,
            workerList = workerList,
            timeout = as.integer(timeout),
            type = type,
            id = id
        ),
        class = "RedisBackend"
    )

    .setClientName(x, clientName)
    if(type == "worker"){
        .addWorkerToJob(x)
    }else{
        .delete(x, workerList)
    }
x
}


.jobCacheQueue <- function(id){
    paste0("job_cache_queue:", id)
}

.clientNamePrefix <-
    function(jobname, type)
{
    paste0(jobname, "_redis_", type, "_")
}

.clientName <-
    function(jobname, type, id)
{
    paste0(.clientNamePrefix(jobname, type), id)
}

## regmatches
.allWorkers <-
    function(x)
{
    prefix <- .clientNamePrefix(x$jobname, "worker")
    clients <- .listClients(x)
    idx <- gregexpr(paste0(" name=", prefix, ".+? "), clients)
    clientsNames <- regmatches(clients, idx)[[1]]
    ## Remove the prefix and the tailing space
    substring(clientsNames, nchar(prefix) + 7, nchar(clientsNames) - 1)
}

.wait_until_success <-
    function(expr, timeout,
             errorMsg, operationWhileWaiting = NULL)
{
    frame <- parent.frame()
    expr <- substitute(expr)
    operationWhileWaiting <- substitute(operationWhileWaiting)
    start_time <- Sys.time()
    repeat{
        .value <- eval(expr, envir = frame)
        if (!is.null(.value)) {
            break
        }
        eval(operationWhileWaiting, envir = frame)
        wait_time <- difftime(Sys.time(), start_time, units = 'secs')
        if (wait_time > timeout) {
            stop(errorMsg)
        }
    }
    .value
}

## The Redis APIs
.setClientName <- function(x, name){
    x$api_client$CLIENT_SETNAME(name)
}

.listClients <- function(x){
    x$api_client$CLIENT_LIST()
}

.quit <- function(x){
    x$api_client$QUIT()
}

.push_raw <- function(x, queue, value){
     x$api_client$LPUSH(
        key = queue,
        value = value
    )
}

.pop_raw <- function(x, queue, timeout = 1L){
     x$api_client$BRPOP(
            key = queue,
            timeout = timeout
        )
}

.move <- function(x, source, dest, timeout = 1L){
    value <- x$api_client$BRPOPLPUSH(
        source = source,
        destination = dest,
        timeout = timeout
    )
    if(is.null(value))
        NULL
    else
        unserialize(value)
}

.delete <- function(x, key){
    x$api_client$DEL(key)
}

.queueLen <- function(x, key){
    x$api_client$LLEN(key)
}

.setAdd <- function(x, key, values){
    x$api_client$SADD(key, values)
}

.setValues <- function(x, key){
    unlist(x$api_client$SSCAN(key, 0, COUNT = .Machine$integer.max)[[2]])
}

.setRemove <- function(x, key, values){
    x$api_client$SREM(key, values)
}

## The high level function built upon the wrappers
.push <-
    function(x, queue, value)
{
    value <- serialize(value, NULL, xdr = FALSE)
    .push_raw(
        x,
        queue = queue,
        value = value
    )
}

.pop <-
    function(x, queue, raw = FALSE)
{
    value <- .wait_until_success(
        .pop_raw(
            x,
            queue = queue
        ),
        timeout = x$timeout,
        errorMsg = "Redis pop operation timeout"
    )
    unserialize(value[[2]])
    }

.addWorkerToJob <- function(x){
    .setAdd(x, x$workerList, x$id)
}

.removeWorkerFromJob <- function(x, worker){
    workerList <- x$workerList
    .setRemove(x, workerList, worker)
}
.resubmit_missing_jobs <- function(x){
    queueName <- x$resultQueue
    workerList <- x$workerList
    connectedWorkers <- bpworkers(x)
    registeredWorkers <- .setValues(x, workerList)
    deadWorkers <- setdiff(registeredWorkers, connectedWorkers)
    ## If there are dead workers, we check if they have any job
    for(id in deadWorkers){
        cacheQueue <- .jobCacheQueue(id)
        queueLen <- .queueLen(x, cacheQueue)
        ## queueLen is 1 means it is running a job
        if (queueLen == 1) {
            message("A missing job is found, resubmitting")
            .move(
                x,
                source = cacheQueue,
                dest = x$jobQueue
            )
        }else{
            if (queueLen != 0L) {
                warning("Corrupted job queue for a worker is found!")
                .delete(x, cacheQueue)
            }
        }
    }
}

.pop_job <- function(x){
    cacheQueue <- .jobCacheQueue(x$id)
    id <- x$id
    .wait_until_success({
        queueName <- ifelse(
            .queueLen(x, id) == 0,
            x$jobQueue,
            id)
        .move(
            x,
            source = queueName,
            dest = cacheQueue
        )
    }
    ,
    timeout = Inf,
    errorMsg = "Redis pop operation timeout"
    )
}

.pop_result <-
    function(x)
{
    value <- .wait_until_success(
        .pop_raw(
            x,
            queue = x$resultQueue
        ),
        timeout = x$timeout,
        errorMsg = "Redis pop operation timeout",
        operationWhileWaiting = .resubmit_missing_jobs(x)
    )
    unserialize(value[[2]])
}

# .push_job <-
#     function(x, value)
# {
#     .push(x, x$jobQueue, value)
# }

.push_result <-
    function(x, value)
{
    cacheQueue <- .jobCacheQueue(x$id)
    .delete(x, cacheQueue)
    .push(x, x$resultQueue, value)
}


#' @export
length.RedisBackend <-
    function(x)
{
    length(bpworkers(x))
}

## Worker

#' @rdname RedisBackend-class
#'
#' @export
setMethod(".recv", "RedisBackend",
    function(worker)
{
    .pop_job(worker)
})

#' @rdname RedisBackend-class
#'
#' @export
setMethod(".send", "RedisBackend",
    function(worker, value)
{
    .push_result(worker, value)
})

#' @rdname RedisBackend-class
#'
#' @export
setMethod(".close", "RedisBackend",
    function(worker)
{
    if (!identical(worker, .redisNULL())) {
        .quit(worker)
    }
    invisible(NULL)
})

## Manager

#' @rdname RedisBackend-class
#'
#' @export
setMethod(".recv_any", "RedisBackend",
    function(backend)
{
    value <- .pop_result(backend)
    list(node = value$tag, value = value)
})

#' @rdname RedisBackend-class
#'
#' @export
setMethod(".send_to", "RedisBackend",
    function(backend, node, value)
{
    node <- bpworkers(backend)[node]
    .push(backend, node, value)
    invisible(TRUE)
})

#' @rdname RedisBackend-class
setMethod(bpjobname, "RedisBackend",
    function(x)
{
    x$jobname
})

#' @rdname RedisBackend-class
setMethod(bpworkers, "RedisBackend",
    function(x)
{
    if (identical(x, .redisNULL())) {
        character()
    } else {
        .allWorkers(x)
    }
})

## Show the job queue status
## For debugging purpose only
bpstatus <- function(x){
    if(is(x, "RedisParam"))
        x <- bpbackend(x)
    jobs <- .queueLen(x, x$jobQueue)
    workers <- .setValues(x, x$workerList)
    workerStatus <- lapply(
        workers,
        function(id) .queueLen(x, .jobCacheQueue(id))
    )
    list(jobs = jobs, workers = workers, workerStatus = workerStatus)
}

