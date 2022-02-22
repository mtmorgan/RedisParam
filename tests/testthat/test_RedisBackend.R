test_that("cleanup Redis", {
    skip_if_not(rpalive())
    manager <- RedisBackend()
    expect_error(.FLUSHALL(manager), NA)
})

test_that("Creating RedisBackend succeeds", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    expect_true(validObject(RedisBackend(jobname = jobname, type = "manager")))
    expect_true(validObject(RedisBackend(jobname = jobname, type = "worker")))

    gc()# clean-up manager & worker
})

test_that("Managers and workers start and end correctly", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisBackend(jobname = jobname, type = "manager")
    workers <- replicate(2L, RedisBackend(jobname = jobname, type = "worker"))

    expect_equal(length(manager), 2L)
    worker_ids <- vapply(workers, `[[`, character(1), "id")
    expect_true(setequal(bpworkers(manager), worker_ids))

    lapply(workers, .QUIT)
    .QUIT(manager)
})

test_that("low-level dispatching function", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisBackend(jobname = jobname, type = "manager")
    worker <- RedisBackend(jobname = jobname, type = "worker")

    ## .send_to
    taskMessage <- BiocParallel:::.EXEC("test", identity,
                                        list(a = runif(1), b = runif(1)),
                                        static.args = "a")
    .send_to(manager, 1, taskMessage)
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 1L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 1L,
             missingTasks = 0L,
             workerNum = 1L)
    )
    expect_equal(
        .rpstatus(worker),
        list(waitingTasks = 1L, runningTasks = 0L)
    )

    ## .recv
    expect_equal(.recv(worker), taskMessage)
    expect_equal(
        .rpstatus(worker),
        list(waitingTasks = 0L, runningTasks = 1L)
    )

    ## .send
    resultMessage <- "result message"
    .send(worker, resultMessage)
    expect_equal(
        .rpstatus(worker),
        list(waitingTasks = 0L, runningTasks = 0L)
    )
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 1L,
             allTasks = 0L,
             missingTasks = 0L,
             workerNum = 1L)
    )

    ## .recv_any
    result <- .recv_any(manager)
    expect_equal(result$node, 1L)
    expect_equal(result$value, resultMessage)

    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 0L,
             missingTasks = 0L,
             workerNum = 1L)
    )

    .QUIT(manager)
    .QUIT(worker)
    gc()
})

test_that("Task manager: basic", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisParam(0, jobname = jobname, is.worker = FALSE)
    worker <- RedisBackend(jobname = jobname, type = "worker")

    bpstart(manager)
    taskManager <- .manager(manager)

    ## .send_to
    taskMessage <- BiocParallel:::.EXEC("test", identity,
                                        list(a = runif(1), b = runif(1)),
                                        static.args = "a")
    .manager_send(taskManager, taskMessage)
    .manager_flush(taskManager)
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 1L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 1L,
             missingTasks = 0L,
             workerNum = 1L)
    )
    expect_equal(
        .rpstatus(worker),
        list(waitingTasks = 0L, runningTasks = 0L)
    )

    ## .recv
    task <- .recv(worker)
    task$data$args <- task$data$args[sort(names(task$data$args))]
    expect_equal(task, taskMessage)
    expect_equal(
        .rpstatus(worker),
        list(waitingTasks = 0L, runningTasks = 1L)
    )

    ## .send
    resultMessage <- "result message"
    .send(worker, resultMessage)
    expect_equal(
        .rpstatus(worker),
        list(waitingTasks = 0L, runningTasks = 0L)
    )
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 1L,
             allTasks = 0L,
             missingTasks = 0L,
             workerNum = 1L)
    )

    ## .recv_any
    result <- .manager_recv(taskManager)
    expect_equal(result[[1]]$value, resultMessage)

    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 0L,
             missingTasks = 0L,
             workerNum = 1L)
    )

    .QUIT(worker)
    gc()
})

test_that("Task manager: auto flush", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisParam(0, jobname = jobname, is.worker = FALSE)
    worker <- RedisBackend(jobname = jobname, type = "worker")

    bpstart(manager)
    taskManager <- .manager(manager)

    ## .send_to
    taskMessage <- BiocParallel:::.EXEC("test", identity,
                                        list(a = runif(1), b = runif(1)),
                                        static.args = "a")
    .manager_send(taskManager, taskMessage)
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 0L,
             missingTasks = 0L,
             workerNum = 1L)
    )
    Sys.sleep(5)
    .manager_send(taskManager, taskMessage)
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 2L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 2L,
             missingTasks = 0L,
             workerNum = 1L)
    )

    ## .recv
    task <- .recv(worker)
    expect_equal(task, taskMessage)
    task <- .recv(worker)
    expect_equal(task, taskMessage)

    .QUIT(worker)
    gc()
})

test_that("Task manager: multiple results", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisParam(0, jobname = jobname, is.worker = FALSE)
    worker <- RedisBackend(jobname = jobname, type = "worker")

    bpstart(manager)
    taskManager <- .manager(manager)

    taskMessage <- BiocParallel:::.EXEC("test", identity,
                                        list(a = runif(1), b = runif(1)),
                                        static.args = "a")
    resultMessage1 <- "result message1"
    resultMessage2 <- "result message2"

    .manager_send(taskManager, taskMessage)
    .manager_send(taskManager, taskMessage)
    .manager_flush(taskManager)

    task <- .recv(worker)
    expect_equal(task, taskMessage)
    .send(worker, resultMessage1)

    task <- .recv(worker)
    expect_equal(task, taskMessage)
    .send(worker, resultMessage2)

    result <- .manager_recv(taskManager)
    expect_equal(result[[1]]$value, resultMessage1)
    expect_equal(result[[2]]$value, resultMessage2)

    .QUIT(worker)
    gc()
})


test_that("Task manager: send all", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisParam(0, jobname = jobname, is.worker = FALSE)
    workers <- replicate(2L, RedisBackend(jobname = jobname, type = "worker"))

    bpstart(manager)
    taskManager <- .manager(manager)

    ## .manager_send_all
    testMessage <- BiocParallel:::.DONE()
    .manager_send_all(taskManager, testMessage)
    .manager_flush(taskManager)
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 2L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 2L,
             missingTasks = 0L,
             workerNum = 2L)
    )
    task1 <- .recv(workers[[1]])
    task2 <- .recv(workers[[2]])
    expect_identical(task1, testMessage)
    expect_identical(task2, testMessage)

    .send(workers[[1]], "DONE1")
    .send(workers[[2]], "DONE2")
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 2L,
             allTasks = 0L,
             missingTasks = 0L,
             workerNum = 2L)
    )
    result <- .manager_recv_all(taskManager)
    expect_true(
        setequal(
            c(result[[1]]$value,result[[2]]$value),
            c("DONE1", "DONE2")
        )
    )

    ## clean up
    .QUIT(workers[[1]])
    .QUIT(workers[[2]])
    gc()
})

test_that("Missing task", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisParam(0, jobname = jobname, is.worker = FALSE)
    workers <- replicate(2L, RedisBackend(jobname = jobname, type = "worker"))

    bpstart(manager)
    taskManager <- .manager(manager)

    ## send a task to the first worker
    taskMessage <- BiocParallel:::.EXEC("test", identity,
                                        list(a = runif(1), b = runif(1)),
                                        static.args = "a")
    .manager_send(taskManager, taskMessage)
    .manager_flush(taskManager)

    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 1L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 1L,
             missingTasks = 0L,
             workerNum = 2L)
    )

    ## .recv
    task <- .recv(workers[[1]])
    .QUIT(workers[[1]])
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 1L,
             missingTasks = 1L,
             workerNum = 1L)
    )

    ## submit twice as the missing value needs
    ## to be checked twice
    .resubmitMissingTasks(bpbackend(manager))
    .resubmitMissingTasks(bpbackend(manager))
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 1L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 1L,
             missingTasks = 0L,
             workerNum = 1L)
    )

    task <- .recv(workers[[2]])
    task$data$args <- task$data$args[sort(names(task$data$args))]
    expect_equal(task, taskMessage)
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 1L,
             finishedTasks = 0L,
             allTasks = 1L,
             missingTasks = 0L,
             workerNum = 1L)
    )
    expect_equal(
        .rpstatus(workers[[2]]),
        list(waitingTasks = 0L, runningTasks = 1L)
    )

    ## .send
    resultMessage <- "result message"
    .send(workers[[2]], resultMessage)
    expect_equal(
        .rpstatus(workers[[2]]),
        list(waitingTasks = 0L, runningTasks = 0L)
    )
    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 1L,
             allTasks = 0L,
             missingTasks = 0L,
             workerNum = 1L)
    )

    ## .recv_any
    result <- .manager_recv(taskManager)
    expect_equal(result[[1]]$value, resultMessage)

    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 0L,
             missingTasks = 0L,
             workerNum = 1L)
    )

    ## clean up
    .QUIT(workers[[2]])
    gc()
})

test_that("Corrupted queue", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisParam(0, jobname = jobname, is.worker = FALSE)
    worker <- RedisBackend(jobname = jobname, type = "worker", log = TRUE)

    bpstart(manager)
    taskManager <- .manager(manager)

    ## send a task to the first worker
    taskMessage <- BiocParallel:::.EXEC("test", identity,
                                        list(a = runif(1), b = runif(1)),
                                        static.args = "a")
    .manager_send(taskManager, taskMessage)
    .manager_flush(taskManager)

    ## .recv
    task <- .recv(worker)
    .FLUSHALL(worker)
    out <- suppressWarnings(capture.output(.send(worker, "test")))
    expect_true(regexpr("WARN", out[1]) == 1)

    expect_equal(
        .rpstatus(manager),
        list(publicTasks = 0L,
             privateTasks = 0L,
             runningTasks = 0L,
             finishedTasks = 0L,
             allTasks = 0L,
             missingTasks = 0L,
             workerNum = 1L)
    )

    ## clean up
    .QUIT(worker)
    gc()
})

test_that("check Redis memery leak", {
    skip_if_not(rpalive())
    manager <- RedisBackend()
    keys <- manager$redisClient$KEYS("*")
    expect_true(length(keys) == 0L)
})
