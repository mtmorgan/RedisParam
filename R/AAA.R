## A thin wrapper for the redux package
## We may remove the redux dependence in future
setOldClass(c("redisNULL", "RedisBackend"))

.RedisParam <- setRefClass(
    "RedisParam",
    contains = "BiocParallelParam",
    fields = c(
        hostname = "character", port = "integer", password = "character",
        backend = "RedisBackend", is.worker = "logical"
    )
)

