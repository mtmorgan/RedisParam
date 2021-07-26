setOldClass(c("redisNULL", "RedisBackend"))

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
        host = rphost(), port = rpport(), password = rppassword(),
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
    clientName <- .clientName(jobname, type, id)
    jobQueue <- paste0("biocparallel_redis_job:", jobname)
    resultQueue <- paste0("biocparallel_redis_result:", jobname)
    workerQueue <-paste0("biocparallel_redis_workers:", jobname)

    api_client <- hiredis(
        host = host,
        port = as.integer(port),
        password = password)

    x <- structure(
        list(
            api_client = api_client,
            clientName = clientName,
            jobname = jobname,
            jobQueue = jobQueue,
            resultQueue = resultQueue,
            workerQueue = workerQueue,
            timeout = as.integer(timeout),
            type = type,
            id = id
        ),
        class = "RedisBackend"
    )

    if (type == "worker")
        .initializeWorker(x)
    else
        .initializeManager(x)

    .setClientName(x, clientName)
    x
}

## Naming rule
.jobCacheQueue <-
    function(id)
{
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

## Utils
.wait_until_success <-
    function(expr, timeout,
             errorMsg, operationWhileWaiting = NULL)
{
    frame <- parent.frame()
    expr <- substitute(expr)
    operationWhileWaiting <- substitute(operationWhileWaiting)
    start_time <- Sys.time()
    repeat {
        .value <- eval(expr, envir = frame)
        if (!is.null(.value))
            break

        eval(operationWhileWaiting, envir = frame)
        wait_time <- difftime(Sys.time(), start_time, units = 'secs')
        if (wait_time > timeout)
            stop(errorMsg)
    }
    .value
}

## Redis APIs
.setClientName <-
    function(x, name)
{
    x$api_client$CLIENT_SETNAME(name)
}

.listClients <-
    function(x)
{
    x$api_client$CLIENT_LIST()
}

.quit <-
    function(x)
{
    x$api_client$QUIT()
    invisible(NULL)
}

.push <-
    function(x, queue, value)
{
    value <- serialize(value, NULL, xdr = FALSE)
    x$api_client$LPUSH(key = queue, value = value)
}

.move <-
    function(x, source, dest, timeout = 1L)
{
    value <- x$api_client$BRPOPLPUSH(
        source = source,
        destination = dest,
        timeout = timeout
    )
    if (is.null(value))
        NULL
    else
        unserialize(value)
}

.delete <-
    function(x, key)
{
    x$api_client$DEL(key)
}

.queueLength <-
    function(x, key)
{
    x$api_client$LLEN(key)
}

.setAdd <-
    function(x, key, values)
{
    x$api_client$SADD(key, values)
}

.setValues <-
    function(x, key)
{
    unlist(x$api_client$SSCAN(key, 0, COUNT = .Machine$integer.max)[[2]])
}

.setRemove <-
    function(x, key, values)
{
    x$api_client$SREM(key, values)
}

## The high level function built upon the wrappers
.initializeWorker <-
    function(x)
{
    if (x$clientName %in% .allWorkers(x))
        stop("Name conflict has been found for the worker '", x$id, "'")

    .setAdd(x, x$workerQueue, x$id)
    cacheQueue <- .jobCacheQueue(x$id)
    if (.queueLength(x, cacheQueue) != 0) {
        warning("An unfinished job has been found for the worker '", x$id, "'")
        .delete(x, cacheQueue)
    }
}

.initializeManager <-
    function(x)
{
    deadWorkers <- .listDeadWorkers(x)
    for (id in deadWorkers) {
        if (.isWorkerBusy(x, id)) {
            warning("An unfinished job has been found for the worker '", id, "'")
        }
        .removeWorkerFromJob(x, id)
    }
    if (.queueLength(x, x$jobQueue) != 0) {
        warning("The job queue is not empty, cleaning up")
        .delete(x, x$jobQueue)
    }
    if (.queueLength(x, x$resultQueue) != 0) {
        warning("The result queue is not empty, cleaning up")
        .delete(x, x$resultQueue)
    }
}

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

.listDeadWorkers <- function(x){
    workers <- .setValues(x, x$workerQueue)
    deadWorkers <- setdiff(workers, bpworkers(x))
    deadWorkers
}

.isWorkerBusy <- function(x, workerId){
    cacheQueue <- .jobCacheQueue(workerId)
    .queueLength(x, cacheQueue) != 0
}

.removeWorkerFromJob <- function(x, workerId){
    cacheQueue <- .jobCacheQueue(workerId)
    .delete(x, cacheQueue)
    workerQueue <- x$workerQueue
    .setRemove(x, workerQueue, workerId)
}

.resubmitMissingJobs <-
    function(x)
{
    deadWorkers <- .listDeadWorkers(x)
    if (length(deadWorkers) > 0)
        message(length(deadWorkers), " workers are missing from the job queue.")

    ## If there are dead workers, we check if they have any job
    for (id in deadWorkers) {
        ## queueLength is 1 means it is running a job
        if (.isWorkerBusy(x, id)) {
            message("A missing job is found, resubmitting")
            cacheQueue <- .jobCacheQueue(id)
            .move(
                x,
                source = cacheQueue,
                dest = x$jobQueue
            )
        }
        .removeWorkerFromJob(x, id)
    }

    invisible(TRUE)
}

.pushJob <-
    function(x, workerId, value)
{
    .push(x, workerId, value)
}

.popJob <-
    function(x)
{
    cacheQueue <- .jobCacheQueue(x$id)
    id <- x$id
    .wait_until_success(
        {
            queueName <- ifelse(
                .queueLength(x, id) == 0,
                x$jobQueue,
                id
            )
            .move(
                x,
                source = queueName,
                dest = cacheQueue
            )
        },
        timeout = Inf,
        errorMsg = "Redis pop operation timeout"
    )
}

.pushResult <-
    function(x, value)
{
    cacheQueue <- .jobCacheQueue(x$id)
    .delete(x, cacheQueue)
    .push(x, x$resultQueue, value)
}

.popResult <-
    function(x, checkTimeout = 1L)
{
    value <- .wait_until_success(
        x$api_client$BRPOP(
            key = x$resultQueue,
            timeout = checkTimeout
        ),
        timeout = x$timeout,
        errorMsg = "Redis pop operation timeout",
        operationWhileWaiting = .resubmitMissingJobs(x)
    )
    unserialize(value[[2]])
}

#' @export
length.RedisBackend <-
    function(x)
{
    length(bpworkers(x))
}

## Worker

#' @rdname RedisBackend-class
setMethod(".recv", "RedisBackend",
    function(worker)
{
    .popJob(worker)
})

#' @rdname RedisBackend-class
setMethod(".send", "RedisBackend",
    function(worker, value)
{
    .pushResult(worker, value)
})

#' @rdname RedisBackend-class
setMethod(".close", "RedisBackend",
    function(worker)
{
    if (!identical(worker, .redisNULL()))
        .quit(worker)
    invisible(NULL)
})

## Manager

#' @rdname RedisBackend-class
setMethod(".recv_any", "RedisBackend",
    function(backend)
{
    value <- .popResult(backend)
    list(node = value$tag, value = value)
})

#' @rdname RedisBackend-class
setMethod(".send_to", "RedisBackend",
    function(backend, node, value)
{
    node <- bpworkers(backend)[node]
    .pushJob(backend, node, value)
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

## Show the backend status
## For debugging purpose only
bpstatus <-
    function(x)
{
    if (is(x, "RedisParam"))
        x <- bpbackend(x)
    njobs <- .queueLength(x, x$jobQueue)
    nresults <- .queueLength(x, x$resultQueue)
    workers <- .setValues(x, x$workerQueue)
    keys <- vapply(workers, .jobCacheQueue, character(1))
    workerStatus <- mapply(.queueLength, workers, keys)
    list(
        njobs = njobs, nresults = nresults,
        workers = workers, workerStatus = workerStatus
    )
}
