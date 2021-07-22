#' @importFrom redux redis_available hiredis
#' @importFrom withr with_envvar
#' @importFrom digest digest
#' @import methods BiocParallel futile.logger
NULL


loadLuaFunctions <- function(){
    scriptPath <- system.file(
        "LuaScript", "functions",
        package="RedisParam"
    )
    files <- list.files(scriptPath, full.names = TRUE)
    scripts <- lapply(files, function(x){
        readChar(x, file.info(x)$size)
    })
    paste0(scripts, collapse = "\r\n")
}

.onLoad <- function(libname, pkgname){
    luaFunctions <- loadLuaFunctions()
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
        script <- paste0(luaFunctions, "\r\n", readChar(x, file.info(x)$size))
        sha1 <- digest::digest(script, algo = "sha1", serialize = FALSE)
        list(value = script, sha1 = sha1)
    })
    for(i in seq_along(scripts)){
        luaScripts[[scriptNames[i]]] <- scripts[[i]]
    }
}
