skip_if_not(rpalive(RedisParam(1L)))

jobname <- "test_RedisBackend"
taskValue <- "task value"
resultValue <- "result value"
manager <- NULL
worker1 <- NULL
worker2 <- NULL



test_that("Creating RedisBackend", {
    expect_error(manager <<- RedisBackend(jobname = jobname, type = "manager", id = "manager1"), NA)
    expect_error(worker1 <<- RedisBackend(jobname = jobname, type = "worker", id = "worker1"), NA)
    expect_error(worker2 <<- RedisBackend(jobname = jobname, type = "worker", id = "worker2"), NA)
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

test_that(".send_to", {
    .send_to(manager, 1, taskValue)
    expect_equal(
        bpstatus(manager),
        list(waitingTask = 1L,
             runningTask = 0L,
             missingTask = 0L,
             doneTask = 0L,
             workerNum = 2L)
    )
    expect_equal(
        bpstatus(worker1),
        list(publicTask = 0L, privateTask = 1L, cache = 0L)
    )
    expect_equal(
        bpstatus(worker2),
        list(publicTask = 0L, privateTask = 0L, cache = 0L)
    )
})

test_that(".recv", {
    ## Worker1 will receive the task
    expect_equal(.recv(worker1), taskValue)
    expect_identical(
        bpstatus(worker1),
        list(publicTask = 0L, privateTask = 0L, cache = 1L)
    )
})

test_that(".resubmitMissingJobs", {
    ## kill worker1's connection to Redis
    worker1 <<- NULL
    gc()
    expect_equal(
        bpstatus(manager),
        list(waitingTask = 0L,
             runningTask = 0L,
             missingTask = 1L,
             doneTask = 0L,
             workerNum = 1L)
    )
    .resubmitMissingJobs(manager)
    expect_equal(
        bpstatus(manager),
        list(waitingTask = 1L,
             runningTask = 0L,
             missingTask = 0L,
             doneTask = 0L,
             workerNum = 1L)
    )
    ## Let worker2 take over the task
    expect_equal(.recv(worker2), taskValue)
    expect_identical(
        bpstatus(worker2),
        list(publicTask = 0L, privateTask = 0L, cache = 1L)
    )
})


test_that(".pushResult", {
    expect_error(.pushResult(worker2, resultValue), NA)
    expect_identical(
        bpstatus(worker2),
        list(publicTask = 0L, privateTask = 0L, cache = 0L)
    )
    expect_equal(
        bpstatus(manager),
        list(waitingTask = 0L,
             runningTask = 0L,
             missingTask = 0L,
             doneTask = 1L,
             workerNum = 1L)
    )
})


test_that(".recv_any", {
    ## .recv_any returns the data in a special format
    ## we use its internal function instead
    expect_equal(.popResult(manager), resultValue)
    expect_equal(
        bpstatus(manager),
        list(waitingTask = 0L,
             runningTask = 0L,
             missingTask = 0L,
             doneTask = 0L,
             workerNum = 1L)
    )
})


