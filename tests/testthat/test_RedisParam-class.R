test_that("RedisParam constructor works", {
    p <- RedisParam()
    expect_true(validObject(p))
    expect_true(validObject(p))
    expect_identical("127.0.0.1", .redis_host(p))
    expect_identical(6379L, .redis_port(p))
    expect_true(is.null(.redis_password(p)))
    expect_identical(FALSE, bpisup(p))

    p <- withr::with_envvar(
        c(REDIS_HOST = "1.2.3.4", REDIS_PORT = 123L, REDIS_PASSWORD = "123"),
        RedisParam()
    )
    expect_identical("1.2.3.4", .redis_host(p))
    expect_identical(123L, .redis_port(p))
    expect_identical("123", .redis_password(p))
})
