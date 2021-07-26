test_that("Creating RedisBackend succeeds", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    expect_true(validObject(RedisBackend(jobname = jobname, type = "manager")))
    expect_true(validObject(RedisBackend(jobname = jobname, type = "worker")))

    gc() # clean-up manager & worker
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
    manager <- RedisBackend(jobname = jobname, type = "manager")
    worker <- RedisBackend(jobname = jobname, type = "worker")

    jobValue <- "job message"
    expect_true(.send_to(manager, 1, jobValue))
    expect_equal(.recv(worker), jobValue)

    resultValue <- "result message"
    expect_equal(.pushResult(worker, resultValue), 1L)
    expect_equal(.popResult(manager), resultValue)

    expect_null(.quit(worker))
    expect_null(.quit(manager))
})

test_that("Job management", {
    skip_if_not(rpalive())

    jobname <- BiocParallel::ipcid()
    manager <- RedisBackend(jobname = jobname, type = "manager")
    workers <- replicate(2L, RedisBackend(jobname = jobname, type = "worker"))

    jobValue <- "job message"
    expect_true(.send_to(manager, 1, jobValue))
    expect_equal(.recv(workers[[1]]), jobValue)

    ## kill worker 1
    .quit(workers[[1]])
    result <-.resubmitMissingJobs(manager)

    ## Receive the job from worker 2
    expect_equal(.recv(workers[[2]]), jobValue)

    ## return result to manager
    resultValue <- "result message"
    expect_identical(.pushResult(workers[[2]], resultValue), 1L)
    expect_equal(.popResult(manager), resultValue)

    .quit(manager)
    expect_error(.quit(workers[[1]])) # already closed
    .quit(workers[[2]])
})
