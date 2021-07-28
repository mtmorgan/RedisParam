## Loaded in .onLoad
luaScripts <- new.env()

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
        timeout = 2592000L, type = c("manager", "worker"), id = NULL,
        workerOffset = NULL, RNGseed = FALSE, log = FALSE
    )
{
    if (!is.null(RedisParam)) {
        jobname <- bpjobname(RedisParam)
        host <- rphost(RedisParam)
        port <- rpport(RedisParam)
        password <- rppassword(RedisParam)
        timeout <- bptimeout(RedisParam)
        log <- bplog(RedisParam)
        RNGseed <- !is.null(bpRNGseed(RedisParam))
    }
    type <- match.arg(type)
    if (is.null(id)) {
        id <- Sys.getenv("REDISPARAM_ID", BiocParallel::ipcid())
    }
    if (is.null(workerOffset)) {
        workerOffset <- sample.int(10000, 1)
    }

    api_client <- hiredis(
        host = host,
        port = as.integer(port),
        password = password)

    clientName <- .clientName(jobname, type, id)
    clients <- api_client$CLIENT_LIST()
    if (grepl(clientName, clients, fixed = TRUE)) {
        stop("Name conflict has been found for the manager/worker '", id, "'")
    }
    api_client$CLIENT_SETNAME(clientName)

    x <- structure(
        list(
            api_client = api_client,
            jobname = jobname,
            timeout = as.integer(timeout),
            type = type,
            id = id,
            workerOffset = workerOffset,
            RNGseed = RNGseed,
            log = log
        ),
        class = "RedisBackend"
    )
    if (type == "worker") {
        .initializeWorker(x)
    }else{
        .initializeManager(x)
    }
    x
}

## index for the values in the list object `taskId` in Redis
taskEltIdx <- list(
    managerResultQueue = 0L,
    taskValue = 1L,
    workerId = 2L
)

## Naming rule
## The task ID is random
.taskId <-
    function(x)
{
    paste0("RedisParam_task_", BiocParallel::ipcid())
}

.managerTaskSetName <-
    function(managerId)
{
    paste0("RedisParam_manager_task_queue_", managerId)
}

.managerResultQueueName <-
    function(managerId)
{
    paste0("RedisParam_manager_result_queue_", managerId)
}

.publicTaskQueueName <-
    function(jobname)
{
    paste0("RedisParam_public_task_queue_", jobname)
}

.workerTaskQueueName <-
function(workerId)
    {
    paste0("RedisParam_worker_task_queue_", workerId)
}

.workerTaskCacheName <-
    function(workerId)
{
    paste0("RedisParam_worker_task_cache_", workerId)
}

.clientName <-
    function(jobname, type, id)
{
    paste0(jobname, "_redis_", type, "_" , id)
}

## Utils
isNoScriptError <-
    function(e)
{
    grepl("NOSCRIPT", e$message, fixed = TRUE)
}

.wait_until_success <-
    function(expr, timeout, errorMsg, operationWhileWaiting = NULL)
{
    frame <- parent.frame()
    expr <- substitute(expr)
    operationWhileWaiting <- substitute(operationWhileWaiting)
    .value <- NULL
    start_time <- Sys.time()
    repeat {
        .value <- eval(expr, envir = frame)
        if (!is.null(.value))
            break
        eval(operationWhileWaiting, envir = frame)
        waitTime <- difftime(Sys.time(), start_time, units = 'secs')
        if (waitTime > timeout)
            stop(errorMsg)
    }
    .value
}

.serialize <- function(object){
    ## Suppress the warning "'package:stats' may not be available when loading"
    ## in the serialize function
    ## as it does not provides any useful information to the user
    suppressWarnings(serialize(object, NULL, xdr = FALSE))
}

.unserialize <- function(object){
    unserialize(object)
}

## Check .send_to is called inside bploop
.isbploop <- function(calls){
    if (length(calls)<=2) {
        FALSE
    } else {
        identical(
        c("bploop.lapply", "cls", "X", "lapply", "ARGFUN", "BPPARAM"),
        as.character(calls[[length(calls) - 2]]))
    }
}

## Redis functions
.quit <-
    function(x)
{
    x$api_client$QUIT()
    NULL
}
.eval <-
    function(x, scriptName, keys = NULL, args = NULL)
{
    stopifnot(scriptName%in%names(luaScripts))
    script <- luaScripts[[scriptName]]
    tryCatch(
        x$api_client$EVALSHA(script$sha1, length(keys), keys, args),
        error =
            function(e) {
                if (isNoScriptError(e))
                    x$api_client$EVAL(script$value, length(keys), keys, args)
                else
                    stop(e)
            })
}

.move <-
    function(x, source, dest, timeout = 1)
{
    x$api_client$BRPOPLPUSH(
        source = source,
        destination = dest,
        timeout = timeout
    )
}

.pipeGetElt <-
    function(key, idx)
{
    redis$LRANGE(key, idx, idx)
}

.initializeWorker <-
    function(x)
{
    workerTaskCache <- .workerTaskCacheName(x$id)
    x$api_client$DEL(workerTaskCache)
}

.initializeManager <-
    function(x)
{
    managerTaskSet <- .managerTaskSetName(x$id)
    managerResultQueue <- .managerResultQueueName(x$id)
    response <- x$api_client$pipeline(
        waitingTaskNum = redis$SCARD(managerTaskSet),
        resultNum = redis$LLEN(managerResultQueue)
    )
    if (response$waitingTaskNum != 0||
        response$resultNum != 0) {
        message("The manager <", x$id, "> has unfinished jobs. Cleaning up")
        .cleanupManager(x)
    }
}

.cleanupManager <-
    function(x)
{
    ## Completely remove the manager data
    managerTaskSet <- .managerTaskSetName(x$id)
    managerResultQueue <- .managerResultQueueName(x$id)
    waitingTaskIds <- unlist(x$api_client$SMEMBERS(managerTaskSet))
    x$api_client$pipeline(
        redis$DEL(waitingTaskIds),
        redis$DEL(managerTaskSet),
        redis$DEL(managerResultQueue)
    )
    NULL
}

.cleanupWorker <-
    function(x)
{
    ## Completely remove the worker data
    workerTaskCache <- .workerTaskCacheName(x$id)
    workerTaskQueue <- .workerTaskQueueName(x$id)
    x$api_client$pipeline(
        redis$DEL(workerTaskCache),
        redis$DEL(workerTaskQueue)
    )
    NULL
}

.allWorkers <-
    function(x)
{
    prefix <- .clientName(x$jobname, "worker", "")
    clients <- x$api_client$CLIENT_LIST()
    idx <- gregexpr(paste0(" name=", prefix, ".+? "), clients)
    clientsNames <- regmatches(clients, idx)[[1]]
    ## Remove the prefix and the tailing space
    substring(clientsNames, nchar(prefix) + 7, nchar(clientsNames) - 1)
}

.resubmitMissingTasks <-
    function(x)
{
    workerIds <- bpworkers(x)
    managerTaskSet <- .managerTaskSetName(x$id)
    managerResultQueue <- .managerResultQueueName(x$id)
    publicTaskQueue <- .publicTaskQueueName(x$jobname)
    ## Find and resubmit the missing task
    response <- .eval(x,
                         "resubmit_missing_tasks",
                         c(publicTaskQueue, managerTaskSet, managerResultQueue),
                         workerIds)
    missingWorkers <- unlist(response[[1]])
    taskNum <- response[[2]]
    if (length(missingWorkers))
        message(
            length(missingWorkers),
            " tasks are missing from the job and has been resubmitted."
        )
    taskNum
}

.pushJob <-
    function(x, workerId, value)
{
    managerResultQueue <- .managerResultQueueName(x$id)
    taskId <- .taskId(x)
    workerTaskQueue <- ifelse(
        workerId == "public",
        .publicTaskQueueName(x$jobname),
        .workerTaskQueueName(workerId)
    )
    managerTaskSet <- .managerTaskSetName(x$id)
    x$api_client$pipeline(
        redis$RPUSH(taskId, managerResultQueue),
        redis$RPUSH(taskId, .serialize(value)),
        redis$RPUSH(taskId, workerId),
        redis$LPUSH(workerTaskQueue, taskId),
        redis$SADD(managerTaskSet, taskId)
    )
}

.selectTaskQueue <- function(x){
    workerTaskQueue <- .workerTaskQueueName(x$id)
    publicTaskQueue <- .publicTaskQueueName(x$jobname)
    lengths <- x$api_client$pipeline(
        worker = redis$LLEN(workerTaskQueue),
        public = redis$LLEN(publicTaskQueue)
    )
    queueName <- ifelse(
        lengths$worker == 0 && lengths$public != 0,
        publicTaskQueue,
        workerTaskQueue
    )
    waitTime <- ifelse(
        lengths$worker != 0 || lengths$public != 0,
        0,
        1
    )
    list(queueName = queueName, waitTime = waitTime)
}

.popJob <-
    function(x)
{
    ## The function is not an atomic operation.
    ## We should expect that the task can be
    ## invalid when trying to get the value of the task,
    ## so I add this while loop
    existsTask <- FALSE
    while(!existsTask){
        workerTaskCache <- .workerTaskCacheName(x$id)
        taskId <- .wait_until_success(
            {
                queueInfo <- .selectTaskQueue(x)
                .move(
                    x,
                    source = queueInfo$queueName,
                    dest = workerTaskCache,
                    timeout = queueInfo$waitTime
                )
            }
            ,
            timeout = Inf,
            errorMsg = "Redis pop operation timeout"
        )
        response <- x$api_client$pipeline(
            existsTask = redis$EXISTS(taskId),
            value = .pipeGetElt(taskId, taskEltIdx$taskValue),
            redis$LSET(taskId, 2, x$id)
        )
        existsTask <- as.logical(response$existsTask)
    }
    unserialize(response$value[[1]])
}

.pushResult <-
    function(x, value)
{
    workerTaskCache <- .workerTaskCacheName(x$id)
    taskId <- unlist(x$api_client$LRANGE(workerTaskCache,0,0))
    ## If someone messes up the job queue
    if (is.null(taskId))
        return(0)
    response <- x$api_client$pipeline(
        taskExist = redis$EXISTS(taskId),
        resultQueue = .pipeGetElt(taskId, taskEltIdx$managerResultQueue),
        workerId = .pipeGetElt(taskId, taskEltIdx$workerId)
    )
    value <- .serialize(list(taskId = taskId, value = value))
    ## continue only when the task exists and
    ## the worker ID matches the current worker
    if (!response$taskExist || response$workerId[[1]] != x$id)
        return(0)

    x$api_client$pipeline(
        redis$RPUSH(
            response$resultQueue[[1]],
            value),
        redis$DEL(c(workerTaskCache, taskId))
    )[[1]]
}

.popResult <-
    function(x, checkInterval = 1L)
{
    managerTaskSet <- .managerTaskSetName(x$id)
    managerResultQueue <- .managerResultQueueName(x$id)
    response <- .wait_until_success(
        x$api_client$BRPOP(
            key = managerResultQueue,
            timeout = checkInterval
        ),
        timeout = x$timeout,
        errorMsg = "Redis pop operation timeout",
        operationWhileWaiting = {
            taskNum <- .resubmitMissingTasks(x)
            if(taskNum == 0L){
                stop("The job queue has been corrputed!")
            }
        }
    )
    response <- unserialize(response[[2]])
    x$api_client$pipeline(
        redis$DEL(response$taskId),
        redis$SREM(managerTaskSet, response$taskId)
    )
    response$value
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
        .cleanupWorker(worker)
    invisible(NULL)
})

## Manager

#' @rdname RedisBackend-class
setMethod(".recv_any", "RedisBackend",
    function(backend)
{
    tryCatch(
        {
            value <- .popResult(backend)
            list(node = value$tag, value = value)
        },
        interrupt = function(condition){
            .cleanupManager(backend)
        }
    )

})

#' @rdname RedisBackend-class
setMethod(".recv_all", "RedisBackend",
    function(backend)
{
    managerTaskSet <- .managerTaskSetName(backend$id)
    replicate(backend$api_client$SCARD(managerTaskSet),
              .recv_any(backend), simplify=FALSE)
})



#' @rdname RedisBackend-class
setMethod(".send_to", "RedisBackend",
    function(backend, node, value)
{
    tryCatch(
        {
            ## We only dispatch the task to the public queue when
            ## 1. .send_to is called by bploop
            ## 2. The RNGseed is disabled
            if (!backend$RNGseed && .isbploop(sys.calls())) {
                .debug(backend, "A task is sent to the public queue")
                node <- "public"
            } else {
                .debug(backend, "A task is sent to the worker queue")
                allWorkers <- bpworkers(backend)
                idx <- (backend$workerOffset + node - 1)%%length(allWorkers) + 1
                node <- allWorkers[idx]
            }
            .pushJob(backend, node, value)
        },
        interrupt = function(condition){
            .cleanupManager(backend)
        }
    )
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
        ## enforce the worker order
        sort(.allWorkers(x))
    }
})

setMethod(bplog, "RedisBackend",
    function(x)
{
    x$log
})

.workerStatus <- function(x, workerId){
    workerTaskQueue <- .workerTaskQueueName(workerId)
    workerTaskCache <- .workerTaskCacheName(workerId)
    response <- x$api_client$pipeline(
        privateTask = redis$LRANGE(workerTaskQueue, 0, -1),
        workerTaskCache = redis$LRANGE(workerTaskCache, 0, -1)
    )
    response$privateTask <- unlist(response$privateTask)
    response$workerTaskCache <- unlist(response$workerTaskCache)
    response
}

## Show the backend task status
## For debugging purpose only
.rpstatus <-
    function(x)
{
    if (is(x, "RedisParam"))
        x <- bpbackend(x)
    if (identical(x, .redisNULL()))
        return(NULL)

    if (x$type == "manager") {
        workerIds <- bpworkers(x)
        workerNum <- length(workerIds)
        managerTaskSet <- .managerTaskSetName(x$id)
        managerResultQueue <- .managerResultQueueName(x$id)
        finishedTask <- x$api_client$LLEN(managerResultQueue)
        managerTaskIds <- unlist(x$api_client$SMEMBERS(managerTaskSet))
        ## Get the worker id from the task
        commands <- lapply(managerTaskIds,
                           function(i) .pipeGetElt(i, taskEltIdx$workerId))
        taskWorkerIds <- unlist(x$api_client$pipeline(.commands = commands))
        ## calculate public and private task number
        publicTask <- taskWorkerIds == "public"
        publicTaskNum <- sum(publicTask)
        privateTaskNum <- length(taskWorkerIds) - publicTaskNum
        taskWorkerIds <- taskWorkerIds[!publicTask]

        ## Check if the worker is still working on the private task
        ## filter out the dead worker
        uniqueIds <- unique(taskWorkerIds)
        liveWorker <- uniqueIds %in% workerIds
        workerStatus <- lapply(uniqueIds[liveWorker],
                               function(i) .workerStatus(x, i))

        waitingTasks <- unlist(lapply(workerStatus, function(x) x$privateTask))
        runningTasks <- unlist(lapply(workerStatus, function(x) x$workerTaskCache))

        waitingTaskNum <- publicTaskNum + sum(managerTaskIds %in% waitingTasks)
        runningTaskNum <- sum(managerTaskIds %in% runningTasks)
        missingTaskNum <- length(managerTaskIds) - waitingTaskNum - runningTaskNum - finishedTask

        list(
            publicTask = publicTaskNum,
            privateTask = privateTaskNum,
            waitingTask = waitingTaskNum,
            runningTask = runningTaskNum,
            finishedTask = finishedTask,
            missingTask = missingTaskNum,
            workerNum = workerNum)
    } else {
        status <- .workerStatus(x, x$id)
        list(privateTask = length(status$privateTask),
             workerTaskCache = length(status$workerTaskCache))
    }
}

.resetRedis <- function(x){
    if (is(x, "RedisParam"))
        x <- bpbackend(x)
    if (identical(x, .redisNULL()))
        return(NULL)
    x$api_client$FLUSHALL
}
