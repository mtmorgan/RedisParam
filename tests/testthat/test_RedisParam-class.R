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

test_that("RedisParam local workers", {
    skip_if_not(rpalive())

    p <- RedisParam(2L)
    p <- bpstart(p)
    expect_s4_class(p, "RedisParam")
    expect_true(validObject(p))
    expect_true(bpisup(p))
    expect_equal(bpnworkers(p), 2L)

    result <- bplapply(1:2, function(i) Sys.getpid(), BPPARAM = p)
    expect_identical(length(result), 2L)
    ## By default the job is sent to the public queue
    ## it is possible that all tasks are performed by a single worker
    expect_true(!Sys.getpid()%in%unique(unlist(result)))

    p <- bpstop(p)
    expect_s4_class(p, "RedisParam")
    expect_true(validObject(p))
    expect_false(bpisup(p))
})

test_that("RedisParam local workers with a long running job", {
    skip_if_not(rpalive())

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
})

test_that("RedisParam RNGseed test", {
    skip_if_not(rpalive())

    p <- RedisParam(2L, RNGseed = 1)

    ## Check if the task is sent to the private queue
    p <- bpstart(p)
    result <- bplapply(1:2, function(i) Sys.getpid(), BPPARAM = p)
    expect_identical(length(result), 2L)
    expect_identical(length(unique(unlist(result))), 2L)
    p <- bpstop(p)

    ## The result should be reproducible
    p <- bpstart(p)
    result1 <- bplapply(1:2, function(i) runif(1), BPPARAM = p)
    p <- bpstop(p)
    p <- bpstart(p)
    result2 <- bplapply(1:2, function(i) runif(1), BPPARAM = p)
    p <- bpstop(p)
    expect_identical(result1, result2)
})

test_that("RedisParam large loop number test", {
    skip_if_not(rpalive())

    n <- 1000
    p <- RedisParam(2L)
    p <- bpstart(p)

    result <- bplapply(seq_len(n), function(i) i, BPPARAM = p)
    expect_equal(seq_len(n), unlist(result))
})

test_that("RedisParam large loop number test", {
    skip_if_not(rpalive())

    n <- 1000
    p <- RedisParam(2L)
    p <- bpstart(p)

    result <- bplapply(seq_len(n), function(i) i, BPPARAM = p)
    expect_equal(seq_len(n), unlist(result))
    p <- bpstop(p)
})



test_that("RedisParam worker and tasks number mismatch", {
  skip_if_not(rpalive())
  
  n <- 3
  p <- RedisParam(2L)
  p <- bpstart(p)
  bptasks(p) <- n
  
  result <- bplapply(seq_len(n), function(i) i, BPPARAM = p)
  expect_equal(seq_len(n), unlist(result))
})

