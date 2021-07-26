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
    expect_identical(NULL, rppassword(p))

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
