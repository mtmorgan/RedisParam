#' @importFrom redux redis_available hiredis redis
#' @importFrom withr with_envvar
#' @importFrom digest digest
#' @import methods BiocParallel futile.logger
NULL


loadLuaHeaders <- function(){
    scriptPath <- system.file(
        "LuaScript", "headers",
        package="RedisParam"
    )
    files <- list.files(scriptPath, full.names = TRUE)
    scripts <- lapply(files, function(x){
        readChar(x, file.info(x)$size)
    })
    paste0(scripts, collapse = "\r\n")
}

.onLoad <- function(libname, pkgname){
    luaHeaders <- loadLuaHeaders()
    scriptPath <- system.file(
        "LuaScript",
        package="RedisParam"
    )
    files <- setdiff(
        list.files(scriptPath),
        list.dirs(scriptPath, recursive = FALSE, full.names = FALSE)
    )
    filePathes <- file.path(scriptPath, files)
    scriptNames <- gsub(".lua", "", files, fixed = TRUE)
    scripts <- lapply(filePathes, function(x){
        script <- paste0(luaHeaders, "\r\n", readChar(x, file.info(x)$size))
        sha1 <- digest::digest(script, algo = "sha1", serialize = FALSE)
        list(value = script, sha1 = sha1)
    })
    for(i in seq_along(scripts)){
        luaScripts[[scriptNames[i]]] <- scripts[[i]]
    }
}
