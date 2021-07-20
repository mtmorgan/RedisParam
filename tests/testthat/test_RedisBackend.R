skip_if_not(rpalive(RedisParam(1L)))

jobname <- "test_RedisBackend"
manager <- NULL
worker1 <- NULL
worker2 <- NULL

test_that("Creating RedisBackend", {
    expect_error(manager <<- RedisBackend(jobname = jobname, type = "manager"), NA)
    expect_error(worker1 <<- RedisBackend(jobname = jobname, type = "worker"), NA)
    expect_error(worker2 <<- RedisBackend(jobname = jobname, type = "worker"), NA)
})

test_that("Check workers", {
    expect_equal(length(manager), 2L)
    expect_true(
        setequal(
            bpworkers(manager),
            c(worker1$id,worker2$id)
        )
    )
})

test_that("job dispatching function", {
    jobValue <- "job message"
    resultValue <- "result message"
    expect_error(.send_to(manager, 1, jobValue), NA)
    expect_equal(.recv(worker1), jobValue)
    expect_error(.pushResult(worker1, resultValue), NA)
    expect_equal(.popResult(manager), resultValue)
})

test_that("Job management", {
    jobValue <- "job message"
    resultValue <- "result message"
    expect_error(.send_to(manager, 1, jobValue), NA)
    expect_equal(.recv(worker1), jobValue)
    ## kill worker1
    .close(worker1)
    .resubmitMissingJobs(manager)
    ## Receive the job from worker2
    expect_equal(.recv(worker2), jobValue)
    expect_error(.pushResult(worker2, resultValue), NA)
    expect_equal(.popResult(manager), resultValue)
})
