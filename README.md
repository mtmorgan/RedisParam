# RedisParam

RedisParam implements a [BiocParallel][] backend using redis, rather than sockets, for communication. It requires a redis server; see `?RedisParam` for host and port specification. redis is a good solution for cloud-based environments using a standard docker image. A particular feature is that the number of workers can be scaled during computation, for instance in response to kubernetes auto-scaling.

[BiocParallel]: https://bioconductor.org/packages/BiocParallel

# Use

Ensure that a redis server is running, e.g., from the command line

```
$ redis-server
```

On a single computer, in _R_, load and use the RedisParam package in the same way as other BiocParallel backends, e.g.,

```{r}
library(RedisParam)
p <- RedisParam(workers = 5)
result <- bplapply(1:7, function(i) Sys.getpid(), BPPARAM = p)
table(unlist(result))
```

For independently managed workers, start workers in separate processes, e.g.,

```{r}
Sys.getpid()
library(RedisParam)
p <- RedisParam(jobname = "demo", is.worker = TRUE)
bpstart(p)
```

Start and use the manager in a separate process. Be sure to use the same `jobname =`.

```{r}
Sys.getpid()            # e.g., 8563
library(RedisParam)
p <- RedisParam(jobname = 'demo', is.worker = FALSE)
result <- bplapply(1:7, function(i) Sys.getpid(), BPPARAM = p)
unique(unlist(result)) # e.g., 9677
```

Independently started workers can be terminated from the manager

```{r}
bpstopall(p)
```

# More information

See the GitHub vignette, or use `browseVignettes(package = "RedisParam")` if RedisParam is installed on your system.

