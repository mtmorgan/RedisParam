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
        timeout = 2592000L, type = c("manager", "worker"), id = NULL
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
    if(is.null(id)){
        id <- Sys.getenv("REDISPARAM_ID", ipcid())
    }
    clientName <- .clientName(jobname, type, id)

    api_client <- hiredis(
        host = host,
        port = as.integer(port),
        password = password)

    x <- structure(
        list(
            api_client = api_client,
            clientName = clientName,
            jobname = jobname,
            timeout = as.integer(timeout),
            type = type,
            id = id
        ),
        class = "RedisBackend"
    )
    clients <- x$api_client$CLIENT_LIST()
    if(grepl(clientName, clients, fixed = TRUE)){
        stop("The client <", clientName, "> has been registered in Redis")
    }
    if(type == "worker"){
        .initializeWorker(x)
    }else{
        .initializeManager(x)
    }
    .setClientName(x, clientName)
    x
}

## index for the values in the list taskId in Redis
taskEltIdx <- list(
    managerResultQueue = 0L,
    taskValue = 1L,
    workerId = 2L
)

## Naming rule
## Randomly generated task ID
.taskId <- function(x){
    paste0("RedisParam_task_", ipcid())
}

.managerTaskSetName <- function(managerId){
    paste0("RedisParam_manager_task_queue_", managerId)
}

.managerResultQueueName <- function(managerId){
    paste0("RedisParam_manager_result_queue_", managerId)
}

.publicTaskQueueName <- function(jobname){
    paste0("RedisParam_public_task_queue_", jobname)
}

.workerTaskQueueName <- function(workerId){
    paste0("RedisParam_worker_task_queue_", workerId)
}

.workerTaskCacheName <- function(workerId){
    paste0("RedisParam_worker_task_cache_", workerId)
}

.clientName <-
    function(jobname, type, id)
{
    paste0(jobname, "_redis_", type, "_" , id)
}

## Utils
isNoScriptError <- function(e){
    grepl("NOSCRIPT", e$message, fixed = TRUE)
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

.serialize <- function(object){
    serialize(object, NULL, xdr = FALSE)
}

.unserialize <- function(object){
    unserialize(object)
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

.eval <- function(x, scriptName, keys = NULL, args = NULL){
    stopifnot(scriptName%in%names(luaScripts))
    script <- luaScripts[[scriptName]]
    tryCatch(
        x$api_client$EVALSHA(script$sha1, length(keys), keys, args),
        error =
            function(e) {
                if(isNoScriptError(e)){
                    x$api_client$EVAL(script$value, length(keys), keys, args)
                }else{
                    stop(e)
                }
            })
}

.move <-
    function(x, source, dest, timeout = 1)
{
    value <- x$api_client$BRPOPLPUSH(
        source = source,
        destination = dest,
        timeout = timeout
    )
    if(is.null(value))
        NULL
    else
        value
}

.pipeGetElt <- function(key, idx){
    redis$LRANGE(key, idx, idx)
}


## The high level function built upon the wrappers
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

.cleanupManager <- function(x){
    managerTaskSet <- .managerTaskSetName(x$id)
    managerResultQueue <- .managerResultQueueName(x$id)
    waitingTaskIds <- unlist(x$api_client$SMEMBERS(managerTaskSet))
    x$api_client$pipeline(
        redis$DEL(waitingTaskIds),
        redis$DEL(managerTaskSet),
        redis$DEL(managerResultQueue)
    )
}

.cleanupWorker <- function(x){
    workerTaskCache <- .workerTaskCacheName(x$id)
    workerTaskQueue <- .workerTaskQueueName(x$id)
    redis$pipeline(
        redis$QUIT(),
        redis$DEL(workerTaskCache),
        redis$DEL(workerTaskQueue)
    )
}

.allWorkers <-
    function(x)
{
    prefix <- .clientName(x$jobname, "worker", "")
    clients <- .listClients(x)
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
    publicTaskQueue <- .publicTaskQueueName(x$jobname)
    missingWorkers <- .eval(x,
                         "resubmit_missing_tasks",
                         c(publicTaskQueue, managerTaskSet),
                         workerIds)
    missingWorkers <- unlist(missingWorkers)
    if(length(missingWorkers)){
        message(length(missingWorkers), " tasks are missing from the job and has been resubmitted.")
        missingWorkerTaskQueues <- .workerTaskQueueName(missingWorkers)
        x$api_client$pipeline(redis$DEL(missingWorkerTaskQueues))
    }
    missingWorkers
}

.pushJob <-
    function(x, workerId, value)
{
    managerResultQueue <- .managerResultQueueName(x$id)
    taskId <- .taskId(x)
    workerTaskQueue <- .workerTaskQueueName(workerId)
    managerTaskSet <- .managerTaskSetName(x$id)
    x$api_client$pipeline(
        redis$RPUSH(taskId, managerResultQueue),
        redis$RPUSH(taskId, .serialize(value)),
        redis$RPUSH(taskId, workerId),
        redis$LPUSH(workerTaskQueue, taskId),
        redis$SADD(managerTaskSet, taskId)
    )
}

.selectQueue <- function(x){
    workerTaskQueue <- .workerTaskQueueName(x$id)
    publicTaskQueue <- .publicTaskQueueName(x$jobname)
    lengths <- x$api_client$pipeline(
        worker = redis$LLEN(workerTaskQueue),
        public = redis$LLEN(publicTaskQueue)
    )
    if(lengths$worker == 0 && lengths$public != 0){
        queueName <- publicTaskQueue
    }else{
        queueName <- workerTaskQueue
    }
    if(lengths$worker != 0 || lengths$public != 0){
        waitTime <- 0.01
    }else{
        waitTime <- 1
    }
    list(queueName = queueName, waitTime = waitTime)
}

.popJob <-
    function(x)
{
    ## The function is not an atomic operation
    ## We should expect that the task can be
    ## invalid when trying to get the value of the task
    existsTask <- FALSE
    while(!existsTask){
        workerTaskCache <- .workerTaskCacheName(x$id)
        taskId <- .wait_until_success({
            queueInfo <- .selectQueue(x)
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
    if(is.null(taskId)){
        return()
    }
    response <- x$api_client$pipeline(
        taskExist = redis$EXISTS(taskId),
        resultQueue = .pipeGetElt(taskId, taskEltIdx$managerResultQueue),
        workerId = .pipeGetElt(taskId, taskEltIdx$workerId)
    )
    value <- .serialize(list(taskId = taskId, value = value))
    if(response$taskExist &&
       response$workerId[[1]] == x$id){
        x$api_client$pipeline(
            redis$RPUSH(
                response$resultQueue[[1]],
                value),
            redis$DEL(c(workerTaskCache, taskId))
        )
    }
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
        operationWhileWaiting = .resubmitMissingTasks(x)
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
#'
#' @export
setMethod(".recv", "RedisBackend",
    function(worker)
{
    .popJob(worker)
})

#' @rdname RedisBackend-class
#'
#' @export
setMethod(".send", "RedisBackend",
    function(worker, value)
{
    .pushResult(worker, value)
})

#' @rdname RedisBackend-class
#'
#' @export
setMethod(".close", "RedisBackend",
    function(worker)
{
    if (!identical(worker, .redisNULL())) {
        .cleanupWorker(worker)
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
#'
#' @export
setMethod(".send_to", "RedisBackend",
    function(backend, node, value)
{
    tryCatch(
        {
            node <- bpworkers(backend)[node]
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
        .allWorkers(x)
    }
})

## Show the backend status
## For debugging purpose only
bpstatus <-
    function(x)
{
    if(is(x, "RedisParam"))
        x <- bpbackend(x)

    if(x$type == "manager"){
        workerIds <- bpworkers(x)
        workerNum <- length(workerIds)
        managerTaskSet <- .managerTaskSetName(x$id)
        managerResultQueue <- .managerResultQueueName(x$id)
        doneTasksNum <- x$api_client$LLEN(managerResultQueue)
        waitingTaskIds <- x$api_client$SMEMBERS(managerTaskSet)
        ## Get the worker id
        commands <- lapply(waitingTaskIds,
                           function(i) .pipeGetElt(i, taskEltIdx$workerId))
        taskWorkerIds <- unlist(x$api_client$pipeline(.commands = commands))
        waitingTaskNum <- sum(taskWorkerIds == "")
        taskWorkerIds <- taskWorkerIds[taskWorkerIds != ""]
        runningTaskNum <- sum(taskWorkerIds%in%workerIds)
        missingTaskNum <- length(taskWorkerIds) - runningTaskNum
        list(waitingTask = waitingTaskNum,
             runningTask = runningTaskNum,
             missingTask = missingTaskNum,
             doneTask = doneTasksNum,
             workerNum = workerNum)
    }else{
        publicTaskQueue <- .publicTaskQueueName(x$jobname)
        workerTaskQueue <- .workerTaskQueueName(x$id)
        workerTaskCache <- .workerTaskCacheName(x$id)
        x$api_client$pipeline(
            publicTask = redis$LLEN(publicTaskQueue),
            privateTask = redis$LLEN(workerTaskQueue),
            cache = redis$LLEN(workerTaskCache)
        )
    }
}
