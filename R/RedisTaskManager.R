#' @include RedisParam-class.R

.RedisManager <- setClass("RedisManager", contains = "environment")

setMethod(".manager", "RedisParam",
          function(BPPARAM)
{
    manager <- .RedisManager()
    manager$BPPARAM <- BPPARAM
    manager$backend <- bpbackend(BPPARAM)
    manager$initialized <- FALSE
    manager$taskQueue <- list()
    manager$flushTime <- Sys.time()
    manager$nworkers <- length(manager$backend)
    manager$workerUpdataTime <- Sys.time()
    manager
})

setMethod(".manager_send", "RedisManager",
    function(manager, value, ...)
{
    manager$taskQueue <- append(manager$taskQueue, list(value))
    flushInterval <- manager$backend$flushInterval
    if (difftime(Sys.time(), manager$flushTime, units = "secs") > flushInterval)
        .manager_flush(manager)
})

setMethod(".manager_recv", "RedisManager",
    function(manager)
{
    x <- manager$backend
    .popResults(x, maxResults = 1024)
})

setMethod(
    ".manager_flush", "RedisManager",
    function(manager)
{
    if (length(manager$taskQueue) > 0L) {
        x <- manager$backend
        publicJobQueue <- .publicJobQueueName(x$jobname)

        if (!manager$initialized) {
            constData <- .task_const(manager$taskQueue[[1]])
        } else {
            constData <- NULL
        }

        dynamicData <- lapply(manager$taskQueue, .task_dynamic)

        .pushTasks(x, dynamicData,
                   jobQueueName = publicJobQueue,
                   isPublic = TRUE,
                   constEnabled = TRUE,
                   pushConst = !manager$initialized,
                   constData = constData)

        if (!manager$initialized) {
            manager$initialized <- TRUE
        }

        manager$taskQueue <- list()
        manager$flushTime <- Sys.time()
    }
})

setMethod(
    ".manager_capacity", "RedisManager",
    function(manager)
{
    if (difftime(Sys.time(), manager$workerUpdataTime, units = "secs") > 1L){
        manager$workerUpdataTime <- Sys.time()
        manager$nworkers <- length(manager$backend)
    }
    ceiling(manager$nworkers * manager$BPPARAM$queue.multiplier)
})

setMethod(
    ".manager_cleanup", "RedisManager",
    function(manager)
{
    x <- manager$backend
    ## worker cleanup
    .send_all(x, .taskCleanup())

    ## manager cleanup
    managerConstQueue <- .managerConstQueueName(x$id)
    .DEL(x, managerConstQueue)

    manager$initialized <- FALSE
})



