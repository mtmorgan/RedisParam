test_that("Creating RedisBackend succeeds", {
    skip_if_not(rpalive(RedisParam(1L)))

    jobname <- BiocParallel::ipcid()
    expect_true(validObject(RedisBackend(jobname = jobname, type = "manager")))
    expect_true(validObject(RedisBackend(jobname = jobname, type = "worker")))

    gc()
})

test_that("Managers and workers start and end correctly", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisBackend(jobname = jobname, type = "manager")
    workers <- replicate(2L, RedisBackend(jobname = jobname, type = "worker"))

    expect_equal(length(manager), 2L)
    worker_ids <- vapply(workers, `[[`, character(1), "id")
    expect_true(setequal(bpworkers(manager), worker_ids))

    result <- lapply(workers, .quit)
    expect_identical(result, rep(list(NULL), 2))
    expect_null(.quit(manager))
})

test_that("job dispatching function", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisBackend(jobname = jobname,type = "manager")
    worker <- RedisBackend(jobname = jobname, type = "worker")

    ## .send_to
    taskMessage <- "task message"
    .send_to(manager, 1, taskMessage)
    expect_equal(
        .rpstatus(manager),
        list(publicTask = 0L,
             privateTask = 1L,
             waitingTask = 1L,
             runningTask = 0L,
             finishedTask = 0L,
             missingTask = 0L,
             workerNum = 1L)
    )
    expect_equal(
        .rpstatus(worker),
        list(privateTask = 1L, workerTaskCache = 0L)
    )

    ## .recv
    expect_equal(.recv(worker), taskMessage)
    expect_equal(
        .rpstatus(worker),
        list(privateTask = 0L, workerTaskCache = 1L)
    )

    ## .send
    resultMessage <- "result message"
    expect_error(.send(worker, resultMessage), NA)
    expect_equal(
        .rpstatus(worker),
        list(privateTask = 0L, workerTaskCache = 0L)
    )
    expect_equal(
        .rpstatus(manager),
        list(publicTask = 0L,
             privateTask = 0L,
             waitingTask = 0L,
             runningTask = 0L,
             finishedTask = 1L,
             missingTask = 0L,
             workerNum = 1L)
    )

    ## .recv_any requires the result has a special format
    ## we use its internal function instead
    expect_equal(.popResult(manager), resultMessage)
    expect_equal(
        .rpstatus(manager),
        list(publicTask = 0L,
             privateTask = 0L,
             waitingTask = 0L,
             runningTask = 0L,
             finishedTask = 0L,
             missingTask = 0L,
             workerNum = 1L)
    )

    .quit(manager)
    .quit(worker)
})

test_that("job management", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisBackend(jobname = jobname, type = "manager", workerOffset = 0L)
    workers <- replicate(2L, RedisBackend(jobname = jobname, type = "worker"))

    ## Find the worker order in the manager
    workerIds <- vapply(workers, `[[`, character(1), "id")
    worker1 <- workers[[which(workerIds == bpworkers(manager)[1])]]
    worker2 <- workers[[which(workerIds == bpworkers(manager)[2])]]

    ## send a task to the first worker
    taskMessage <- "task message"
    expect_true(.send_to(manager, 1, taskMessage))
    expect_equal(
        .rpstatus(worker1),
        list(privateTask = 1L, workerTaskCache = 0L)
    )
    expect_equal(.recv(worker1), taskMessage)
    expect_equal(
        .rpstatus(worker1),
        list(privateTask = 0L, workerTaskCache = 1L)
    )

    ## kill worker1
    .quit(worker1)
    expect_equal(
        .rpstatus(manager),
        list(publicTask = 0L,
             privateTask = 1L,
             waitingTask = 0L,
             runningTask = 0L,
             finishedTask = 0L,
             missingTask = 1L,
             workerNum = 1L)
    )
    .resubmitMissingTasks(manager)
    expect_equal(
        .rpstatus(manager),
        list(publicTask = 1L,
             privateTask = 0L,
             waitingTask = 1L,
             runningTask = 0L,
             finishedTask = 0L,
             missingTask = 0L,
             workerNum = 1L)
    )

    ## Receive the job from worker 2
    expect_equal(.recv(worker2), taskMessage)
    expect_equal(
        .rpstatus(worker2),
        list(privateTask = 0L, workerTaskCache = 1L)
    )

    ## Return result to manager
    resultValue <- "result message"
    expect_identical(.send(worker2, resultValue), 1L)
    expect_equal(.popResult(manager), resultValue)

    ## clean up
    .quit(manager)
    .quit(worker2)
})


