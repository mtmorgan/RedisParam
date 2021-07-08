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
    ## We only do the test if Redis is running
    p <- RedisParam()
    redisAlive <- tryCatch({
        hiredis(
            host = rphost(p),
            port = rpport(p),
            password = rppassword(p)
        )
        TRUE
    }, error = function(e) FALSE)
    if (redisAlive) {
        expect_true(bpstart(p))
        expect_true(bpisup(p))
        expect_error(bplapply(1:2, function(i) Sys.getpid(), BPPARAM = p), NA)
        expect_true(bpstop(p))
    }
})
