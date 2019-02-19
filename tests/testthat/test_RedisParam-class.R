context("RedisParam-class")

test_that("RedisParam constructor works", {
    p <- RedisParam()
    expect_true(validObject(p))
    expect_identical(BiocParallel::snowWorkers(), bpnworkers(p))
    expect_identical("127.0.0.1", .redis_host(p))
    expect_identical(6379L, .redis_port(p))

    p <- RedisParam(2)
    expect_true(validObject(p))
    expect_identical(2L, bpnworkers(p))
})
