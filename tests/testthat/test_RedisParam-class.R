test_that("RedisParam constructor works", {
    ## Test if the environment variables work as we expect
    p <- withr::with_envvar(
        c(REDIS_HOST = "1.2.3.4", REDIS_PORT = 123L, REDIS_PASSWORD = "123"),
        RedisParam()
    )
    expect_identical("1.2.3.4", .redis_host(p))
    expect_identical(123L, .redis_port(p))
    expect_identical("123", .redis_password(p))

    ## Test the default constructor
    ## There might exist environment variables
    p <- RedisParam()
    expect_true(validObject(p))
    expect_identical(FALSE, bpisup(p))
})


test_that("RedisParam local workers", {
    ## Check if the redis server exists
    p <- RedisParam()
    redisAlive <- tryCatch({
        hiredis(host = .redis_host(p),
                port = .redis_port(p),
                password = .redis_password(p)
        )
        TRUE
    }, error = function(e) FALSE)
    if(redisAlive){
        expect_true(bpstart(p))
        expect_true(bpisup(p))
        expect_error(bplapply(1:2, function(i) Sys.getpid(), BPPARAM = p), NA)
        expect_true(bpstop(p))
    }
})
