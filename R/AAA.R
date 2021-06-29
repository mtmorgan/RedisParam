## A thin wrapper for the redux package
## We may remove the redux dependence in future
#' @importFrom redux redis_available hiredis
.RedisBackend <- setRefClass(
    "RedisBackend",
    fields = c(
        api_client = "ANY",
        job_name = "character",
        job_queue = "character",
        result_queue = "character",
        timeout = "integer",
        type = "character"
    )
)


#' @import methods BiocParallel
.RedisParam <- setRefClass(
    "RedisParam",
    contains = "BiocParallelParam",
    fields = c(
        hostname = "character", port = "integer", password = "character",
        backend = "RedisBackend", is.worker = "logical"
    ),
    methods = list(
        show = function() {
            callSuper()
            .password <- "*****"
            if (is.null(.redis_password(.self)))
                .password <- NA_character_
            cat(
                "  hostname: ", .redis_host(.self), "\n",
                "  port: ", .redis_port(.self),
                "; password: ", .password,
                "; is.worker: ", .redis_isworker(.self), "\n",
                sep = "")
        }
    )
)

