# Getting started

RedisParam implements a [BiocParallel][] backend using redis, rather
than sockets, for communication. It requires a redis server; see
`?RedisParam` for host and port specification. redis is a good
solution for cloud-based environments using a standard docker image. A
particular feature is that the number of workers can be scaled during
computation, for instance in response to kubernetes auto-scaling.

[BiocParallel]: https://bioconductor.org/packages/BiocParallel

# Use

Ensure that a redis server is running, e.g., from the command line

```
$ redis-server
```

## Manager and workers from a single _R_ session

On a single computer, in _R_, load and use the RedisParam package in
the same way as other BiocParallel backends, e.g.,


```r
library(RedisParam)
```

```
## Loading required package: BiocParallel
```

```r
p <- RedisParam(workers = 5)
result <- bplapply(1:7, function(i) Sys.getpid(), BPPARAM = p)
table(unlist(result))
```

```
## 
## 66060 66062 66064 66066 
##     2     1     2     2
```

## Independently-managed workers

For independently managed workers, start workers in separate processes, e.g.,


```r
Sys.getpid()
library(RedisParam)
p <- RedisParam(jobname = "demo", is.worker = TRUE)
bpstart(p)
```

Start and use the manager in a separate process. Be sure to use the
same `jobname =`.


```r
Sys.getpid()            # e.g., 8563
library(RedisParam)
p <- RedisParam(jobname = 'demo', is.worker = FALSE)
result <- bplapply(1:7, function(i) Sys.getpid(), BPPARAM = p)
unique(unlist(result)) # e.g., 9677
```

Independently started workers can be terminated from the manager


```r
rpstopall(p)
```

# Session info {.unnumbered}

This version of the vignette was built on 2022-06-08 with the
following software package versions:


```
## R version 4.2.0 (2022-04-22)
## Platform: aarch64-apple-darwin20 (64-bit)
## Running under: macOS Monterey 12.4
## 
## Matrix products: default
## BLAS:   /Library/Frameworks/R.framework/Versions/4.2-arm64/Resources/lib/libRblas.0.dylib
## LAPACK: /Library/Frameworks/R.framework/Versions/4.2-arm64/Resources/lib/libRlapack.dylib
## 
## locale:
## [1] en_US.UTF-8/en_US.UTF-8/en_US.UTF-8/C/en_US.UTF-8/en_US.UTF-8
## 
## attached base packages:
## [1] stats     graphics  grDevices utils     datasets  methods   base     
## 
## other attached packages:
## [1] RedisParam_0.99.0   BiocParallel_1.30.3
## 
## loaded via a namespace (and not attached):
##  [1] rstudioapi_0.13      knitr_1.39           magrittr_2.0.3      
##  [4] R6_2.5.1             rlang_1.0.2          fastmap_1.1.0       
##  [7] stringr_1.4.0        tools_4.2.0          parallel_4.2.0      
## [10] xfun_0.31            cli_3.3.0            withr_2.5.0         
## [13] lambda.r_1.2.4       futile.logger_1.4.3  jquerylib_0.1.4     
## [16] htmltools_0.5.2      yaml_2.3.5           digest_0.6.29       
## [19] formatR_1.12         sass_0.4.1           futile.options_1.0.1
## [22] codetools_0.2-18     evaluate_0.15        rmarkdown_2.14      
## [25] redux_1.1.3          stringi_1.7.6        compiler_4.2.0      
## [28] bslib_0.3.1          jsonlite_1.8.0
```

