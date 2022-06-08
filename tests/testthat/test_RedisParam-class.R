if (rpalive())
    .FLUSHALL(RedisBackend())

checkMemLeak <-
    function()
{
    gc()
    ## some workers are not moving fast enough
    ## be patient
    Sys.sleep(2)
    manager <- RedisBackend()
    keys <- unlist(manager$redisClient$KEYS("*"))
    keys <- keys[!grepl(.redisLogQueue(""), keys)]
    expect_true(length(keys) == 0L)
}

test_that("RedisParam constructor works", {
    ## Test if the environment variables `REDISPARAM_XX` work as we expect
    p <- withr::with_envvar(
        c(REDISPARAM_HOST = "1.2.3.4",
          REDISPARAM_PORT = 123L,
          REDISPARAM_PASSWORD = "123",
          REDIS_HOST = NA,
          REDIS_PORT = NA,
          REDIS_PASSWORD = NA),
        RedisParam()
    )
    expect_identical("1.2.3.4", rphost(p))
    expect_identical(123L, rpport(p))
    expect_identical("123", rppassword(p))

    ## Test if the fallback environment variables `REDIS_XX` work as we expect
    p <- withr::with_envvar(
        c(REDISPARAM_HOST = NA,
          REDISPARAM_PORT = NA,
          REDISPARAM_PASSWORD = NA,
          REDIS_HOST = "1.2.3.4",
          REDIS_PORT = 123L,
          REDIS_PASSWORD = "123"),
        RedisParam()
    )
    expect_identical("1.2.3.4", rphost(p))
    expect_identical(123L, rpport(p))
    expect_identical("123", rppassword(p))

    ## Test if the default constructor work as we expect
    p <- withr::with_envvar(
        c(REDISPARAM_HOST = NA,
          REDISPARAM_PORT = NA,
          REDISPARAM_PASSWORD = NA,
          REDIS_HOST = NA,
          REDIS_PORT = NA,
          REDIS_PASSWORD = NA),
        RedisParam()
    )
    expect_identical("127.0.0.1", rphost(p))
    expect_identical(6379L, rpport(p))
    expect_identical(NA_character_, rppassword(p))

    withr::with_envvar(
        c(REDIS_PORT = "tcp://10.19.242.166:6379L"),
        suppressWarnings({
            expect_error(rpport())
            expect_error(RedisParam())
        })
    )

    ## Test the default constructor
    ## There might exist environment variables
    p <- RedisParam()
    expect_true(validObject(p))
    expect_identical(FALSE, bpisup(p))
})

test_that("RedisParam local workers start and stop", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2L)
    expect_equal(rpattachedworker(p), NA_character_)
    expect_equal(.rpstatus(p)$workerNum, 0L)

    p <- bpstart(p)
    expect_s4_class(p, "RedisParam")
    expect_true(validObject(p))
    expect_true(bpisup(p))
    expect_equal(bpnworkers(p), 2L)
    expect_equal(length(rpattachedworker(p)), 2L)
    expect_equal(.rpstatus(p)$workerNum, 2L)

    p <- bpstop(p)
    Sys.sleep(1)
    expect_s4_class(p, "RedisParam")
    expect_true(validObject(p))
    expect_false(bpisup(p))
    expect_equal(rpattachedworker(p), NA_character_)
    expect_equal(.rpstatus(p)$workerNum, 0L)

    #checkMemLeak()
})

test_that("RedisParam worker auto starts", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2L)

    result <- bplapply(1:2, function(i) Sys.getpid(), BPPARAM = p)
    expect_identical(length(result), 2L)
    ## By default the job is sent to the public queue
    ## it is possible that all tasks are performed by a single worker
    expect_true(!Sys.getpid()%in%unique(unlist(result)))

    Sys.sleep(1)
    expect_equal(rpattachedworker(p), NA_character_)
    expect_equal(.rpstatus(p)$workerNum, 0L)

    #checkMemLeak()
})

test_that("RedisParam stop all workers", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2L)
    p <- bpstart(p)
    expect_equal(.rpstatus(p)$workerNum, 2L)
    rpstopall(p)

    Sys.sleep(2)
    expect_equal(.rpstatus(p)$workerNum, 0L)
    bpstop(p)
    expect_equal(.rpstatus(p)$workerNum, 0L)

    #checkMemLeak()
})

test_that("RedisParam worker killed by the other reason", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())
    jobname <- .randomId()

    p1 <- RedisParam(2L, jobname = jobname)
    p1 <- bpstart(p1)
    expect_equal(.rpstatus(p1)$workerNum, 2L)

    ## different job queue, nothing happens
    p2 <- RedisParam(is.worker = FALSE)
    bpstop(p2)
    expect_equal(.rpstatus(p1)$workerNum, 2L)
    rpstopall(p2)
    Sys.sleep(1)
    expect_equal(.rpstatus(p1)$workerNum, 2L)

    ## Same job queue, kill all workers
    p3 <- RedisParam(is.worker = FALSE, jobname = jobname)
    bpstop(p3)
    expect_equal(.rpstatus(p1)$workerNum, 2L)
    rpstopall(p3)
    Sys.sleep(1)
    expect_equal(.rpstatus(p1)$workerNum, 0L)

    bpstop(p1)
    expect_equal(.rpstatus(p1)$workerNum, 0L)

    #checkMemLeak()
})

test_that("RedisParam local workers with a long running job", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2L)
    p <- bpstart(p)
    expect_equal(bpnworkers(p), 2L)

    result <- bplapply(1:4,
                       function(i) {
                           Sys.sleep(2)
                           Sys.getpid()
                       }, BPPARAM = p)
    expect_identical(length(result), 4L)
    expect_identical(length(unique(unlist(result))), 2L)

    p <- bpstop(p)

    #checkMemLeak()
})

test_that("RedisParam restart cluster", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2L)
    for(i in 1:10)
        result <- bplapply(1:2, function(i) runif(1), BPPARAM = p)

    checkMemLeak()
})

test_that("RedisParam RNGseed test", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2L, RNGseed = 1)
    expect_equal(bpRNGseed(p), 1)

    ## The result should be reproducible
    result1 <- bplapply(1:2, function(i) runif(1), BPPARAM = p)
    result2 <- bplapply(1:2, function(i) runif(1), BPPARAM = p)
    expect_identical(result1, result2)

    #checkMemLeak()
})

test_that("RedisParam RNGseed reset", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2L, RNGseed = 1)

    ## The RNG seed is resettable
    result1 <- bplapply(1:2, function(i) runif(1), BPPARAM = p)
    bpRNGseed(p) <- 1
    result2 <- bplapply(1:2, function(i) runif(1), BPPARAM = p)
    expect_identical(result1, result2)

    #checkMemLeak()
})

test_that("RedisParam log", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2L, RNGseed = 1)

    expect_equal(bplog(p), FALSE)
    bplog(p) <- TRUE
    expect_equal(bplog(p), TRUE)

    #checkMemLeak()
})

test_that("RedisParam large loop number test", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    n <- 1000
    p <- RedisParam(2L)
    result <- bplapply(seq_len(n), function(i) i, BPPARAM = p)
    expect_equal(seq_len(n), unlist(result))

    #checkMemLeak()
})

test_that("RedisParam worker and tasks number mismatch", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    n <- 3
    p <- RedisParam(2L)
    bptasks(p) <- n

    result <- bplapply(seq_len(n), function(i) i, BPPARAM = p)
    expect_equal(seq_len(n), unlist(result))

    #checkMemLeak()
})

test_that("RedisParam worker escapes from manager, auto stop", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2L, jobname = "escape")
    bpstart(p)
    expect_equal(.rpstatus(p)$workerNum, 2L)

    rm(p)
    gc()

    p <- RedisParam(0L, jobname = "escape")
    expect_equal(.rpstatus(p)$workerNum, 0L)

    #checkMemLeak()
})

test_that("RedisParam missing worker", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2)
    bpstart(p)
    expect_equal(.rpstatus(p)$workerNum, 2L)
    pids <- unlist(bplapply(1:2, function(x) Sys.getpid(), BPPARAM = p))
    res <- bplapply(1:2,
                    function(x, id) {
                        if(Sys.getpid()==id)
                            quit()
                        else
                            Sys.getpid()
                    } ,
                    id = pids[1],
                    BPPARAM = p)
    expect_equal(unique(unlist(res)), pids[2])
    expect_equal(.rpstatus(p)$workerNum, 1L)
    bpstop(p)

    #checkMemLeak()
})

test_that("RedisParam server log", {
    skip_if_not(rpalive())
    #.FLUSHALL(RedisBackend())

    p <- RedisParam(2)
    expect_true(length(rplog(p)) == 0)
    bpstart(p)
    rplog(p) <- TRUE
    bplapply(1:2, function(x) Sys.getpid(), BPPARAM = p)
    bpstop(p)
    expect_true(length(rplog(p)) > 0)

    #checkMemLeak()
})

test_that("Check memory leaking", {
    skip_if_not(rpalive())

    Sys.sleep(2)
    manager <- RedisBackend()
    keys <- unlist(manager$redisClient$KEYS("*"))
    keys <- keys[!grepl(.redisLogQueue(""), keys)]
    expect_true(length(keys) == 0L)
})

