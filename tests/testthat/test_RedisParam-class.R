test_that("RedisParam constructor works", {
    ## Test if the environment variables work as we expect
    p <- withr::with_envvar(
        c(REDIS_HOST = "1.2.3.4", REDIS_PORT = 123L, REDIS_PASSWORD = "123"),
        RedisParam()
    )
    expect_identical("1.2.3.4", rphost(p))
    expect_identical(123L, rpport(p))
    expect_identical("123", rppassword(p))

    ## Test the default constructor
    ## There might exist environment variables
    p <- RedisParam()
    expect_true(validObject(p))
    expect_identical(FALSE, bpisup(p))
})


test_that("RedisParam local workers", {
    p <- RedisParam(2L)
    skip_if_not(rpalive(p))

    p <- bpstart(p)
    expect_s4_class(p, "RedisParam")
    expect_true(validObject(p))
    expect_true(bpisup(p))

    result <- bplapply(1:5, function(i) Sys.getpid(), BPPARAM = p)
    expect_identical(length(result), 5L)
    expect_identical(length(unique(unlist(result))), 2L)

    p <- bpstop(p)
    expect_s4_class(p, "RedisParam")
    expect_true(validObject(p))
    expect_false(bpisup(p))
})



test_that("RedisParam local workers with a long running job", {
    p <- RedisParam(2L)
    skip_if_not(rpalive(p))

    p <- bpstart(p)
    expect_s4_class(p, "RedisParam")
    expect_true(validObject(p))
    expect_true(bpisup(p))

    result <- bplapply(1:5,
                       function(i) {
                           sleep(2)
                           Sys.getpid()
                       }, BPPARAM = p)
    expect_identical(length(result), 5L)
    expect_identical(length(unique(unlist(result))), 2L)

    p <- bpstop(p)
    expect_s4_class(p, "RedisParam")
    expect_true(validObject(p))
    expect_false(bpisup(p))
})
