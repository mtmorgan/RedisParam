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
#'
#' @param id character(1) The manager/worker ID. If not given by the
#'     user and the environment `REDISPARAM_ID` is not defined, a
#'     random ID will be used
#'
#' @param log logical(1) Whether to enable the log
#'
#' @param redis.log logical(1) Whether to enable the redis server log
#'
#' @param flushInterval numeric(1) The waiting time between two flush operation.
#'
#' @return `RedisBackend()` returns an object of class
#'     `RedisBackend`. This object is not useful to the end user.
RedisBackend <-
    function(
        RedisParam = NULL, jobname = "myjob",
        host = rphost(), port = rpport(), password = rppassword(),
        timeout = .Machine$integer.max,
        type = c("manager", "worker"), id = NULL,
        log = FALSE, redis.log = NULL,
        flushInterval = 5L
    )
{
    if (!is.null(RedisParam)) {
        jobname <- bpjobname(RedisParam)
        host <- rphost(RedisParam)
        port <- rpport(RedisParam)
        password <- rppassword(RedisParam)
        timeout <- bptimeout(RedisParam)
        log <- bplog(RedisParam)
    }
    type <- match.arg(type)

    if (is.null(id))
        id <- Sys.getenv("REDISPARAM_ID", .randomId())
    if (is.null(redis.log))
        redis.log <- Sys.getenv("REDISPARAM_REDIS_LOG") != ""
    if (is.na(password))
        password <- NULL

    redisClient <-
        hiredis(host = host, port = as.integer(port), password = password)
    clientName <- .clientName(jobname, type, id)
    clients <- redisClient$CLIENT_LIST()
    if (grepl(clientName, clients, fixed = TRUE))
        stop("Name conflict has been found for the manager/worker '", id, "'")
    redisClient$CLIENT_SETNAME(clientName)

    x <- structure(list(
        redisClient = redisClient, jobname = jobname,
        timeout = as.integer(timeout), type = type, id = id,
        log = log, redis.log = redis.log, flushInterval = flushInterval,
        workingSpace = new.env(parent = emptyenv())
    ), class = "RedisBackend")

    if (type == "worker")
        .initializeWorker(x)
    else
        .initializeManager(x)
    x
}

.randomId <-
    function(len = 10L)
{
    id <- gsub("-", "", BiocParallel::ipcid(), fixed = TRUE)
    substr(id, 1, len)
}

## Naming rule
.taskId <-
    function(managerId, isPublic, constEnabled)
{
    paste0(
        ifelse(isPublic, "pub", "pri"), "_",
        ifelse(constEnabled, "sta", "non"), "_",
        .randomId(), "_",
        managerId
    )
}

.taskInfo <-
    function(taskId)
{
    x <- strsplit(taskId, "_", fixed = TRUE)[[1]]
    isPublic <- x[1] == "pub"
    constEnabled <- x[2] == "sta"
    managerId <- paste0(x[-seq_len(3)], collapse = "_")
    list(
        isPublic = isPublic, managerId = managerId, constEnabled = constEnabled
    )
}

.redisLogQueue <-
    function(jobname)
{
    paste0("RP_log_queue_", jobname)
}

.workerTaskMap <-
    function(jobname)
{
    paste0("RP_worker_map_", jobname)
}

.publicJobQueueName <-
    function(jobname)
{
    paste0("RP_public_queue_", jobname)
}

.privateJobQueueName <-
    function(workerId)
{
    paste0("RP_private_queue_", workerId)
}

.managerTaskSetName <-
    function(managerId)
{
    paste0("RP_manager_task_queue_", managerId)
}

.managerResultQueueName <-
    function(managerId)
{
    paste0("RP_manager_result_queue_", managerId)
}

.managerConstQueueName <-
    function(managerId)
{
    paste0("RP_manager_const_data_", managerId)
}

.clientName <-
    function(jobname, type, id)
{
    paste0(jobname, "_redis_", type, "_" , id)
}

## Return non-null value from expr to break the loop
.waitUntilSuccess <-
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
        if (!is.na(timeout) && waitTime > timeout)
            stop(errorMsg)
    }
    .value
}

.serialize <- function(object){
    serialize(object, NULL, xdr = FALSE)
}

.unserialize <- function(object){
    unserialize(object)
}

## special task
.taskCleanup <-
    function()
{
    paste0("RP_worker_self_cleanup")
}

## utilities
## Redis pipeline commands
.cleanTaskCommands <-
    function(x)
{
    taskId <- x$workingSpace$taskId
    managerId <- x$workingSpace$managerId
    managerTaskSet <- .managerTaskSetName(managerId)
    workerTaskMap <- .workerTaskMap(x$jobname)

    ## delete the task data and remove it from the
    ## manager
    cmd1 <- NULL
    if (!is.null(taskId))
        cmd1 <-
        list(
            redis$DEL(taskId),
            redis$SREM(managerTaskSet, taskId)
        )

    ## delete the worker-task link
    cmd2 <- list(redis$HDEL(workerTaskMap, x$id))

    c(cmd1, cmd2)
}

.initializeWorker <-
    function(x)
{
    .cleanupWorker(x)
}

.initializeManager <-
    function(x)
{
    .cleanupManager(x)
}

.cleanupManager <-
    function(x)
{
    ## Completely remove the manager data
    managerTaskSet <- .managerTaskSetName(x$id)
    managerResultQueue <- .managerResultQueueName(x$id)
    managerConstQueue <- .managerConstQueueName(x$id)

    ## get all task names for this manager
    taskIds <- unlist(.SMEMBERS(x, managerTaskSet))

    .DEL(
        x,
        c(taskIds, managerTaskSet, managerResultQueue, managerConstQueue)
    )
}

.cleanupWorker <-
    function(x)
{
    ## Completely remove the worker private queue
    privateJobQueue <- .privateJobQueueName(x$id)
    workerTaskMap <- .workerTaskMap(x$jobname)
    cmd1 <- list(redis$DEL(privateJobQueue))

    ## remove the current task(if any)
    cmd2 <- .cleanTaskCommands(x)

    ## send out all commands
    cmds <- c(cmd1, cmd2)
    .pipeline(x, .commands = cmds)
}

.allWorkers <-
    function(x, clients)
{
    if (missing(clients)){
        clients <- x$redisClient$CLIENT_LIST()
    }
    prefix <- .clientName(x$jobname, "worker", "")
    idx <- gregexpr(paste0(" name=", prefix, ".+? "), clients)
    clientsNames <- regmatches(clients, idx)[[1]]
    ## Remove the prefix and the tailing space
    substring(clientsNames, nchar(prefix) + 7, nchar(clientsNames) - 1)
}

.cleanMissingWorkers <-
    function(x)
{
    workerTaskMap <- .workerTaskMap(x$jobname)

    response <-
        .pipeline(
            x,
            recordedWorker = redis$HKEYS(workerTaskMap),
            clients = redis$CLIENT_LIST()
        )
    recordedWorker <- unlist(response$recordedWorker)
    workers <- .allWorkers(x, clients = response$clients)

    missingWorkers <- setdiff(recordedWorker, workers)
    if (length(missingWorkers))
        .HDEL(x, workerTaskMap, missingWorkers)
}

.resubmitMissingTasks <-
    function(x)
{
    managerTaskSet <- .managerTaskSetName(x$id)
    publicJobQueue <- .publicJobQueueName(x$jobname)
    workerTaskMap <- .workerTaskMap(x$jobname)
    workers <- .allWorkers(x)

    response <- .pipeline(
        x,
        .commands = list(
            runningTasks = redis$HMGET(workerTaskMap, workers),
            waitingTasks = redis$LRANGE(publicJobQueue, 0, -1),
            allTasks = redis$SMEMBERS(managerTaskSet)
        )
    )

    allTasks <- unlist(response$allTasks)
    runningTasks <- unlist(response$runningTasks)
    waitingTasks <- unlist(response$waitingTasks)

    ## determine if the task is public
    isPublic <- unlist(lapply(allTasks, function(i) .taskInfo(i)$isPublic))
    publicTasks <- allTasks[isPublic]

    ## only trace the public missing task
    existingTasks <- c(runningTasks, waitingTasks)
    missingTasks <- setdiff(publicTasks, existingTasks)

    ## A task is claimed to be missing if it is missing in
    ## two consecutive check
    trueMissing <- intersect(x$workingSpace$missingTasks, missingTasks)
    x$workingSpace$missingTasks <- missingTasks

    if (length(trueMissing) > 0) {
        cmds <- lapply(trueMissing, function(i) redis$LPUSH(publicJobQueue, i))
        .pipeline(x, .commands = cmds)
        message(
            length(trueMissing),
            " tasks are missing from the job queue and has been resubmitted."
        )
    }

    length(allTasks)
}

.pushTasks <-
    function(
        x, values, jobQueueName, isPublic = TRUE, constEnabled = TRUE,
        pushConst = TRUE, constData = NULL)
{
    managerId <- x$id
    managerTaskSet <- .managerTaskSetName(managerId)
    managerConstQueue <- .managerConstQueueName(managerId)

    ## If required, we push the const data to Redis server
    cmd1 <- NULL
    if (constEnabled && pushConst) {
        cmd1 <- list(
            redis$DEL(managerConstQueue),
            redis$LPUSH(managerConstQueue, .serialize(constData))
            )
    }

    ## prepare the commands for sending the task value to Redis
    taskIds <- replicate(
        length(values),
        .taskId(managerId, isPublic, constEnabled)
    )
    cmd2 <- lapply(seq_along(taskIds), function(i) {
        value <- values[[i]]
        taskId <- taskIds[[i]]
        list(
            redis$LPUSH(taskId, .serialize(value)),
            redis$LPUSH(jobQueueName, taskId),
            redis$SADD(managerTaskSet, taskId)
        )
    })

    cmds <- c(cmd1, unlist(cmd2, recursive = FALSE))
    .pipeline(x, .commands = cmds)

    for (i in taskIds)
        .redisLog(
            x, "%s  Manager %s pushes task %s to %s. const: %d",
            Sys.time(), x$id, i, jobQueueName, pushConst
        )

    taskIds
}

.popTask_id <-
    function(x)
{
    ## Obtain the task Id
    privateJobQueue <- .privateJobQueueName(x$id)
    publicJobQueue <- .publicJobQueueName(x$jobname)
    .waitUntilSuccess(
        .BRPOP(x, c(privateJobQueue, publicJobQueue), 1L)[[2]],
        timeout = Inf,
        errorMsg = "Redis pop operation timeout"
    )
}

.popTask_response <-
    function(x, managerId, taskId, loadConst)
{
    ## load the const data if required
    cmd1 <- NULL
    if (loadConst) {
        constQueue <- .managerConstQueueName(managerId)
        cmd1 <- list(const = redis$LRANGE(constQueue, 0, 0))
    }

    ## load the task value
    cmd2 <- list(value = redis$LRANGE(taskId, 0, 0))

    ## register the worker task link
    workerTaskMap <- .workerTaskMap(x$jobname)
    cmd3 <- list(redis$HSET(workerTaskMap, x$id, taskId))

    ## compose the response
    .pipeline(x, .commands = c(cmd1, cmd2, cmd3))
}

.popTask <-
    function(x)
{
    ## extract parameters from task
    taskId <- .popTask_id(x)
    taskInfo <- .taskInfo(taskId)
    managerId <- taskInfo$managerId
    constEnabled <- taskInfo$constEnabled

    ## The worker needs to know this info when sending back the result
    ## to the manager
    x$workingSpace$managerId <- managerId
    x$workingSpace$taskId <- taskId

    ## compose the response
    loadConst <- constEnabled && is.null(x$workingSpace$cache[[managerId]])
    response <- .popTask_response(x, managerId, taskId, constEnabled)

    ## unserialize the task
    if (length(response$value) == 0) {
        fmt <- "Worker %s: The task %s is missing, something is wrong"
        .warn(x, fmt, x$id, taskId)
        return(.popTask(x))
    }
    value <- .unserialize(response$value[[1]])

    ## If this is a cleanup task, we cleanup the worker and
    ## call another pop function
    if (identical(value, .taskCleanup())) {
        x$workingSpace$cache[[managerId]] <- NULL
        .pipeline(x, .commands = .cleanTaskCommands(x))
        return(.popTask(x))
    }

    ## load the const data
    if (loadConst) {
        const <- .unserialize(response$const[[1]])
        x$workingSpace$cache[[managerId]] <- const
    }

    ## remake the task
    value <- .task_remake(value, x$workingSpace$cache[[managerId]])

    fmt <- "%s  Worker %s receives task %s. const: %d"
    .redisLog(x, fmt, Sys.time(), x$id, taskId, constEnabled)

    value
}

.pushResult <-
    function(x, value)
{
    taskId <- x$workingSpace$taskId
    managerId <- x$workingSpace$managerId
    managerResultQueue <- .managerResultQueueName(managerId)
    managerTaskSet <- .managerTaskSetName(managerId)

    value <- .serialize(list(taskId = taskId, value = value))

    ## commands for pushing the result back to the manager
    ## only happens when the task ID is still hold by the manager
    cmd1 <- NULL
    taskExists <- .SISMEMBER(x, managerTaskSet, taskId)
    if (taskExists)
        cmd1 <- list(redis$RPUSH(managerResultQueue, value))

    ## commands for deleting the task
    cmd2 <- .cleanTaskCommands(x)

    .pipeline(x, .commands = c(cmd1, cmd2))

    ## server log
    if (taskExists) {
        .redisLog(
            x, "%s  Worker %s pushes result %s to %s",
            Sys.time(), x$id, taskId, managerId
        )
    }else{
        .warn(
            x, "Worker %s discards result %s",
            x$id, taskId
        )
        .redisLog(
            x, "%s  Worker %s discards result %s",
            Sys.time(), x$id, taskId
        )
    }
    NULL
}

.popResults <-
    function(x, maxResults = 1, checkInterval = 5L)
{
    managerResultQueue <- .managerResultQueueName(x$id)

    ## Use a double pop trick to pop all results
    ## from the queue with a blocking operation
    cmd1 <- list(redis$BLPOP(managerResultQueue, checkInterval))

    cmd2 <- NULL
    if (maxResults > 1L)
        cmd2 <- list(
            redis$LRANGE(managerResultQueue, 0, maxResults - 1L),
            redis$DEL(managerResultQueue)
        )

    cmds <- c(cmd1, cmd2)

    values <- .waitUntilSuccess(
        {
            response <- .pipeline(x, .commands = cmds)
            if(is.null(response[[1]]))
                NULL
            else if (maxResults == 1L)
                response[[1]][2]
            else
                c(response[[1]][2], response[[2]])
        },
        timeout = x$timeout,
        errorMsg = "Redis pop operation timeout",
        operationWhileWaiting = {
            taskNum <- .resubmitMissingTasks(x)
            if (taskNum == 0L)
                .error(x, "The job queue has been corrputed!")
        }
    )

    values <- values[lengths(values) != 0L]
    results <- lapply(values, .unserialize)

    .redisLog(
        x, "%s  Manager %s receives %d result",
        Sys.time(), x$id, length(results)
    )

    results
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
    .popTask(worker)
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
setMethod(".send_to", "RedisBackend",
    function(backend, node, value)
{
    tryCatch(
        {
            if (is.numeric(node)) {
                allWorkers <- bpworkers(backend)
                privateJobQueue <- .privateJobQueueName(allWorkers[node])
            } else {
                privateJobQueue <- .privateJobQueueName(node)
            }
            taskId <- .pushTasks(backend, list(value),
                                jobQueueName = privateJobQueue,
                                isPublic  = FALSE,
                                constEnabled = FALSE)
            backend$workingSpace[[taskId]] <- node
        },
        interrupt = function(condition){
            .cleanupManager(backend)
        }
    )
    invisible(TRUE)
})

#' @rdname RedisBackend-class
setMethod(".recv_any", "RedisBackend",
    function(backend)
{
    tryCatch(
        {
            response <- .popResults(backend, maxResults = 1)[[1]]
            node <- backend$workingSpace[[response$taskId]]
            rm(list = response$taskId, envir = backend$workingSpace)
            list(node = node, value = response$value)
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
    managerResultQueue <- .managerResultQueueName(backend$id)
    ntasks <- sum(unlist(
        .pipeline(
            backend, redis$SCARD(managerTaskSet),
            redis$LLEN(managerResultQueue)
        )
    ))
    replicate(ntasks, .recv_any(backend), simplify=FALSE)
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

.rpstatus_manager <-
    function(x)
{
    publicJobQueue <- .publicJobQueueName(x$jobname)
    managerResultQueue <- .managerResultQueueName(x$id)
    managerTaskSet <- .managerTaskSetName(x$id)
    workerTaskMap <- .workerTaskMap(x$jobname)

    workers <- .allWorkers(x)

    ## Commands for obtaining the public job info
    cmds <- list(
        publicTasks = redis$LRANGE(publicJobQueue, 0, -1),
        finishedTasks = redis$LLEN(managerResultQueue),
        allTasks = redis$SMEMBERS(managerTaskSet)
    )
    if (length(workers))
        cmds$runningTasks <- redis$HMGET(workerTaskMap, workers)
    response <- .pipeline(x, .commands = cmds)

    publicTasks <- response$publicTasks
    finishedTasks <- response$finishedTasks
    allTasks <- response$allTasks
    runningTasks <- unlist(response$runningTasks)

    ## Commands for obtaining the private job info
    cmds <- lapply(workers, function(i) {
        redis$LRANGE(.privateJobQueueName(i), 0 , -1)
    })
    privateTasks <- unlist(x$redisClient$pipeline(.commands = cmds))

    workerNum <- length(workers)

    missingTasks <-
        setdiff(allTasks,
                c(publicTasks, privateTasks, publicTasks, runningTasks))

    list(
        publicTasks = length(publicTasks),
        privateTasks = length(privateTasks),
        runningTasks = length(runningTasks),
        finishedTasks = finishedTasks,
        allTasks = length(allTasks),
        missingTasks = length(missingTasks),
        workerNum = workerNum
    )
}

.rpstatus_worker <-
    function(x)
{
    privateJobQueue <- .privateJobQueueName(x$id)
    workerTaskMap <- .workerTaskMap(x$jobname)
    response <- .pipeline(
        x,
        waitingTasks = redis$LLEN(privateJobQueue),
        runningTasks = redis$HEXISTS(workerTaskMap, x$id)
    )
    list(
        waitingTasks = response$waitingTasks,
        runningTasks = as.integer(response$runningTasks)
    )
}

.rpstatus <-
    function(x)
{
    if (is(x, "RedisParam")) {
        if (!bpisup(x)) {
            .bpstart_redis_manager(x)
            param <- x
            on.exit(bpbackend(param) <- .redisNULL())
        }
        x <- bpbackend(x)
    }
    if (x$type == "manager") {
        .rpstatus_manager(x)
    } else {
        .rpstatus_worker(x)
    }
}

rplog <-
    function(x)
{
    if (is(x, "RedisParam")) {
        param <- x
        if (!bpisup(param)){
            if (!rpalive(param))
                return()
            .bpstart_redis_manager(param)
            on.exit(bpbackend(param) <- .redisNULL())
        }
        x <- bpbackend(param)
    }

    unlist(x$redisClient$LRANGE(.redisLogQueue(x$jobname), 0, -1))
}

`rplog<-` <-
    function(x, value)
{
    bpbackend(x)$redis.log <- value
    x
}
